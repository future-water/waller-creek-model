// Simple Code of Sending depth & battery data to AWS databases with Adaptive_sampling_time 

// Public domain (CC0) 
// Can be used in open or closed-source commercial projects and derivative works without attribution.

// Tested with Device OS 1.4.4
// - Electron U260
// - Boron LTE

#include "Particle.h"

// EXAMPLE USAGE
#define MAX_MEASUREMENT_SIZE 50
#define MAX_HOST_SIZE 20
#define MAX_REGION_SIZE 50
#define MAX_VALUE_SIZE 4
#define MAX_DATABUFFER_SIZE 1500
#define MAX_RESPONSE_SIZE 1500
#define MAX_DATA_SIZE 1500
#define MAX_RECV_SIZE 128
#define MAX_ITER 20


TCPClient client;

/////This part needs to be written for server and database connection. 
//char *server=""; // AWS
//char *user = "";
//char *password = "";
//char *db = "";
//int port = ;
/////////////////////////////////////////////////////////////////////////

//char *data_buffer ="depth,host=particle_01,region=us-east value=66.33";
//set up the data format for collection
char measurement_depth_name[MAX_MEASUREMENT_SIZE]={"depth"};
char measurement_bat_name[MAX_MEASUREMENT_SIZE]={"v_bat"};

char host[MAX_HOST_SIZE] = {"battery_test"};
char value[MAX_VALUE_SIZE] = {"\0"};
char data_buffer[300]={'\0'};

// Buffers
char measurement_name[MAX_MEASUREMENT_SIZE] = {'\0'};
char response_buffer[MAX_RESPONSE_SIZE] = {'\0'};
uint8_t recv[MAX_RECV_SIZE] = {0};

//adeptive_sampling_time 
int status = 1;
const pin_t MY_LED = D7;

// This example uses threading enabled and SEMI_AUTOMATIC mode
SYSTEM_THREAD(ENABLED);
SYSTEM_MODE(SEMI_AUTOMATIC);

// If you are using a product, uncomment these lines and set the correct product ID and version
// for your product
// PRODUCT_ID(8761);
// PRODUCT_VERSION(4);

// Using Serial1 (RX/TX) for debugging logs and an external TTL serial to USB (FT232) converter
// is useful when testing sleep modes. Sleep causes USB serial to disconnect, and you will often
// lose the debug logs immediately after wake. With an external USB serial converter, your
// serial terminal stays connected so you get all log messages. If you don't have one, you can
// comment out the Serial1LogHandler and uncomment the SerialLogHandler to use USB.
Serial1LogHandler logHandler(115200);
// SerialLogHandler logHandler;

// This is the maximum amount of time to wait for the cloud to be connected in
// milliseconds. This should be at least 5 minutes. If you set this limit shorter,
// on Gen 2 devices the modem may not get power cycled which may help with reconnection.
const std::chrono::milliseconds connectMaxTime = 10min;

// This is the minimum amount of time to stay connected to the cloud. You can set this
// to zero and the device will sleep as fast as possible, however you may not get 
// firmware updates and device diagnostics won't go out all of the time. Setting this
// to 10 seconds is typically a good value to use for getting updates.
const std::chrono::milliseconds cloudMinTime = 1min;

// How long to sleep
//const std::chrono::seconds sleepTime = 30min;
auto sleepTime = std::chrono::minutes (1);


// Maximum time to wait for publish to complete. It normally takes 20 seconds for Particle.publish
// to succeed or time out, but if cellular needs to reconnect, it could take longer, typically
// 80 seconds. This timeout should be longer than that and is just a safety net in case something
// goes wrong.
const std::chrono::milliseconds publishMaxTime = 3min;

// Maximum amount of time to wait for a user firmware download in milliseconds
// before giving up and just going back to sleep
const std::chrono::milliseconds firmwareUpdateMaxTime = 5min;

// These are the states in the finite state machine, handled in loop()
enum State {
    STATE_WAIT_CONNECTED = 0,
    STATE_PUBLISH,
    STATE_PRE_SLEEP,
    STATE_SLEEP,
    STATE_FIRMWARE_UPDATE
};
State state = STATE_WAIT_CONNECTED;
int stateTime;
bool firmwareUpdateInProgress = false;

void readSensorAndPublish(); // forward declaration
void readBatteryAndPublish();
void firmwareUpdateHandler(system_event_t event, int param); // forward declaration

void setup()
{
  // Make sure your Serial Terminal app is closed before powering your device
  Serial.begin(9600);
  Serial1.begin(9600);
  //Adaptive LED
  //pinMode(MY_LED, OUTPUT);
  
  /*
    FuelGauge fuel;
    if (fuel.getSoC() < 15) {
        // If battery is too low, don't try to connect to cellular, just go back into
        // sleep mode.
        Log.info("low battery, going to sleep immediately");
        state = STATE_SLEEP;
        return;
    }
    */

    System.on(firmware_update, firmwareUpdateHandler);

    // It's only necessary to turn cellular on and connect to the cloud. Stepping up
    // one layer at a time with Cellular.connect() and wait for Cellular.ready() can
    // be done but there's little advantage to doing so.
    Cellular.on();
    Particle.connect();
    stateTime = millis();
}


void loop() {
    switch(state) {
        case STATE_WAIT_CONNECTED:
            // Wait for the connection to the Particle cloud to complete
            if (Particle.connected()) {
                Log.info("connected to the cloud in %lu ms", millis() - stateTime);
                state = STATE_PUBLISH; 
                stateTime = millis(); 
            }
            else
            if (millis() - stateTime >= connectMaxTime.count()) {
                // Took too long to connect, go to sleep
                Log.info("failed to connect, going to sleep");
                state = STATE_SLEEP;
            }
            break;

        case STATE_PUBLISH: //When board is working 
            readSensorAndPublish();
            //////////////////////////Adaptive_control_LED////////////////////
            readData(response_buffer, "Adaptive_sampling_time", 1);
            // Parse value from GET request and save result as a variable
            status = parseReading(response_buffer, "Adaptive_sampling_time");
            // Light LED if status != 0
            //digitalWrite(MY_LED, status);
            //light_led(7, 1);
            //////////////////////////////////////////////////////////////////
            
            

            if (millis() - stateTime < cloudMinTime.count()) {
                Log.info("waiting %lu ms before sleeping", cloudMinTime.count() - (millis() - stateTime));
                state = STATE_PRE_SLEEP;
            }
            else {
                state = STATE_SLEEP;
            }
            break;

        case STATE_PRE_SLEEP:
            // This delay is used to make sure firmware updates can start and diagnostics go out
            // It can be eliminated by setting cloudMinTime to 0 and sleep will occur as quickly
            // as possible. 
            if (millis() - stateTime >= cloudMinTime.count()) {
                state = STATE_SLEEP;
            }
            break;

        case STATE_SLEEP:
            if (firmwareUpdateInProgress) {
                Log.info("firmware update detected");
                state = STATE_FIRMWARE_UPDATE;
                stateTime = millis();
                break;
            }

            Log.info("going to sleep for %ld seconds", (long) sleepTime.count());
            
            {
                SystemSleepConfiguration config;
#if HAL_PLATFORM_NRF52840
                // Gen 3 (nRF52840) does not suppport HIBERNATE with a time duration
                // to wake up. This code uses ULP sleep instead. 
                sleepTime = std::chrono::minutes (status); 
                config.mode(SystemSleepMode::ULTRA_LOW_POWER)
                    .duration(sleepTime);
                System.sleep(config);

                // One difference is that ULP continues execution. For simplicity,
                // we just match the HIBERNATE behavior by resetting here.
                System.reset();
#else
                config.mode(SystemSleepMode::HIBERNATE)
                    .duration(sleepTime);
                System.sleep(config);
                // This is never reached; when the device wakes from sleep it will start over 
                // with setup()
#endif
            }
            break; 

        case STATE_FIRMWARE_UPDATE:
            if (!firmwareUpdateInProgress) {
                Log.info("firmware update completed");
                state = STATE_SLEEP;
            }
            else
            if (millis() - stateTime >= firmwareUpdateMaxTime.count()) {
                Log.info("firmware update timed out");
                state = STATE_SLEEP;
            }
            break;
    }
}

uint16_t read_sensor(void) {
char input;       // type for data read
const uint8_t length = 4;           // number of ascii numeric characters in sensor data 
                                    // Expect the UART to contain something like "Sonar..copyright.. \rTempI\rR1478\rR1477\r..."
char buf[length];                   // array to store data
uint8_t i = 0;                      // counter
uint16_t dist = 0;
Serial1.flush();

while (Serial1.available()){
    input = Serial1.read();         // read sensor input
    if (input == 'R') {             // check the first value
      while (i < length) {
        buf[i] = Serial1.read();    // assign input char to buffer index
        i++;
        
       // Serial.println(buf);
      }
    }
}
Serial.println(atoi(buf));
return atoi(buf);
}


void swap(uint16_t  *p, uint16_t  *q) {
    int t;
    
    t = *p;
    *p = *q;
    *q = t;
}


void sort(uint16_t arr[], size_t n) {
    int i, j;
    
    for (i = 0; i < n - 1; i++) {
        for (j = 0; j < n - i - 1; j++) {
            if (arr[j] < arr[j + 1])
                swap(&arr[j], &arr[j + 1]);
        }
    }
}


uint16_t median(uint16_t arr[], size_t size){
   sort(arr, size);
   if (size % 2 != 0)
      return arr[size/2];
   return (arr[(size-1)/2] + arr[size/2])/2;
}


void readSensorAndPublish()
{
uint16_t sensor_readings_arr[30] = {0};

for (int i = 0; i < 5; i++){
    sensor_readings_arr[i] = read_sensor();
    delay(500);
}

uint16_t distance = median(sensor_readings_arr, 5);
//Serial.print("distance: "); Serial.print(distance); Serial.println(" mm"); 

for (int i = 0; i < 5; i++) {
    sensor_readings_arr[i] = 0;
}

float batterySoc = System.batteryCharge();
Serial.println(batterySoc);
TCPconnection(distance, batterySoc);
}



void firmwareUpdateHandler(system_event_t event, unsigned int param) {
    switch(param) {
        case firmware_update_begin:
            firmwareUpdateInProgress = true;
            break;

        case firmware_update_complete:
        case firmware_update_failed:
            firmwareUpdateInProgress = false;
            break;
    }
}





void TCPconnection(float data_1, float data_2)
{
  Serial.println("connecting...");

  if (client.connect(server, port))
  {
    Serial.println("connected");
    //client.println("GET /search?q=unicorn HTTP/1.0");
    //client.println("Host: www.google.com");
    //client.println("Content-Length: 0");
    //client.println();
    
    sprintf(data_buffer,"%s,node_id=%s value=%.2f\n%s,node_id=%s value=%.2f", measurement_depth_name,host,data_1,measurement_bat_name,host,data_2);
    //sprintf(data_buffer,"%s,host=%s,region=%s value=%.2f", measurement_depth_name,host,region,data_1);
    //sprintf(data_buffer,"%s,host=%s,region=%s value=%.2f", measurement_bat_name,host,region,data_2);
    //Serial.print(data_buffer);



    //client.printlnf("POST /write?db=%s HTTP/1.1", db); //#without user, password
    client.printlnf("POST /write?db=%s&u=%s&p=%s HTTP/1.1", db, user, password);
    client.printlnf("Host: %s:%d", server, port);
    client.println("User-Agent: Photon/1.0");
    client.printlnf("Content-Length: %d", strlen(data_buffer));
    client.println("Content-Type: application/x-www-form-urlencoded");
    client.println();
    client.print(data_buffer);
    client.println();
  }
  else
  {
    Serial.println("connection failed");
  }
}

/////////////////////Adaptive control /////////////////////////////////////
int readData(char *response_buffer, char *measurement_name, int verbose){
    //function that reads a measurement from influxdb
    // Send data over to influx
    memset(response_buffer, '\0', MAX_RESPONSE_SIZE);
    if (client.connect(server, port)){
        // Write GET request to client
        client.printlnf("GET /query?q=SELECT%%20last%%28value%%29%%20FROM%%20%s%%20WHERE%%20node_id%%3D%%27%s%%27&db=%s&u=%s&p=%s HTTP/1.1", measurement_name, host, db, user, password);
        client.printlnf("Host: %s:%d", server, port);
        client.println("Connection: close");
        client.println();
        // Read response
        readResponse(response_buffer);
        // If verbose, echo all output to serial
        if (verbose)
        {
            Serial.println("############ BEGIN HTTP GET REQUEST ############");
            Serial.printlnf("GET /query?q=SELECT%%20last%%28value%%29%%20FROM%%20%s%%20WHERE%%20node_id%%3D%%27%s%%27&db=%s&u=%s&p=%s HTTP/1.1", measurement_name, host, db, user, password);
            Serial.printlnf("Host: %s:%d", server, port);
            Serial.println("Connection: close");
            Serial.println();
            Serial.printlnf("############ END HTTP GET REQUEST ############");
            Serial.println();
            // Print response
            Serial.println("############ BEGIN HTTP GET RESPONSE ############");
            Serial.printlnf("%s", response_buffer);
            Serial.printlnf("############ END HTTP GET RESPONSE ############");
            Serial.println();            
        }
        client.stop();
        memset(measurement_name, '\0', MAX_MEASUREMENT_SIZE);
        return 1;
    }
    else
    {
        Serial.println("Connection Failed!");
    }
    memset(measurement_name, '\0', MAX_MEASUREMENT_SIZE);
    return 0;
}

int readResponse(char *response_buffer)
{
    int recv_len = 0;
    int current_response_len = 0;
    char *cursor = response_buffer;
    char *search_ptr = response_buffer;
    char *header_end = 0;
    char *body_end = 0;
    char *section_break = "\r\n\r\n";
    // Keep reading until packet is exhausted
    delay(100);
    for (int i=0; i < MAX_ITER; i++)
    {
        // Read from TCP buffer
        recv_len = client.read(recv, MAX_RECV_SIZE);
        // If nothing was read, set recv_len to 0
        if (recv_len == -1){recv_len = 0;}
        // Prevent buffer overflow
        if (MAX_RESPONSE_SIZE - current_response_len - recv_len <= 0){break;}
        // Copy chunk into response buffer
        memcpy(response_buffer + current_response_len, recv, recv_len);
        // Update response length
        current_response_len += recv_len;
        // Reset receive buffer
        memset(recv, 0, sizeof(recv));
        // Look for section break
        cursor = strstr(search_ptr, section_break);
        // If section break found...
        if (cursor)
        {
            // Update the search pointer
            search_ptr = cursor + strlen(section_break);
            // If it's the first section break, demarcate end of header
            if (!header_end)
            {
                header_end = search_ptr;
            }
            // If its the second section break, stop reading
            else
            {
                break;
            }
        }
        delay(100);
    } 
    return 1;
    
}

//function for LED light turn on : for test // 
void light_led(int pin_number, int status)
{
    if (status=1)
        {
            digitalWrite(pin_number, HIGH);
        }
    else
        {
            digitalWrite(pin_number, LOW);
        }
}

float parseReading(char *buffer, char *search_term){
    char *a = buffer;
    char *b = buffer;
    char value_str[10] = {'\0'};
    float value = 0;
    
    // Search HTTP response for search term
    a = strstr(a, search_term);
    // If found...
    if (a)
    {
        // Set pointer at first character after search term
        a += strlen(search_term);
        // Search remaining string for "values"
        a = strstr(a, "values");
        // If found...
        if (a)
        {
            // Set pointer at first character after "values"
            a += strlen("values");
            // Search remaining string for ","
            a = strstr(a, ",");
            // If found...
            if (a)
            {
                // Set pointer at first character after ","
                a += strlen(",");
                // Search for closing bracket
                b = strstr(a, "]");
                // If found...
                if (b)
                {
                    // Copy all characters between "," and "]" into value_str
                    strncpy(value_str, a, b - a);
                    // Convert value_str to a float
                    value = strtof(value_str, NULL);
                    // Return parsed value
                    return value;
                }
            }
        }
    }
    return 0;
}




