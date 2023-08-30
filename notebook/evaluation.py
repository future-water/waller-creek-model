import numpy as np

def roc_evals(thresholds, residuals,labels):
    detected_outliers = {}
    is_outlier = []
    is_outliers = []
    detected={}
    roc_tpr = []
    roc_fpr = []

    for threshold in thresholds: 
        detected={}
        is_outlier = []
        is_outliers = []
        for j in residuals.values:
            if j > threshold:
                is_outlier = False # Label point as outliers
                is_outliers.append(is_outlier)
            else:
                is_outlier = True # Label point as inlier
                is_outliers.append(is_outlier)

        detected={threshold: [is_outliers.copy()]}
        detected_outliers.update(detected)
        
        detector_result= np.array(detected_outliers[threshold])
        detector_result=detector_result
        label=labels.values

        TP = (detector_result & label).sum()
        FP = (detector_result & ~label).sum()
        P = (label).sum()
        N = (~label).sum()
        TPR = TP / P
        FPR = FP / N
        roc_tpr.append(TPR)
        roc_fpr.append(FPR)
   
    #Calculating AUC value 
    auc=np.trapz(roc_tpr, x=roc_fpr)
    return roc_fpr,roc_tpr, auc
