import numpy as np
import re
import pandas as pd
import matplotlib.pyplot as plt


####ALS 1#####
RMSEALS1 = [ 0.811704,0.811794,0.811481]
MaeALS1 = [0.626872,0.627161,0.626589]



print("Mean:")
RMSEMEANALS1 = np.mean(RMSEALS1)
MaeMEANALS1 = np.mean(MaeALS1)
print(RMSEMEANALS1)
print(MaeMEANALS1)

print("Standard Deviation:")
RMSEALS1STD =np.std(RMSEALS1)
MaeALS1std = np.std(MaeALS1)
print(RMSEALS1STD)
print(MaeALS1std)


######ALS 2#####
RMSEALS2 = [0.809118,0.809124,0.809016]
MaeALS2 = [0.623944, 0.624034,0.623693]

print("Mean:")
RMSEMEANALS2 = np.mean(RMSEALS2)
MaeMEANALS2= np.mean(MaeALS2)
print(RMSEMEANALS2)
print(MaeMEANALS2)


print("Standard Deviation:")
RMSEAL2STD =np.std(RMSEALS2)
MaeALS2std = np.std(MaeALS2)
print(RMSEAL2STD)
print(MaeALS2std)



#####ALS 3 ######
RMSEALS3 = [0.841579,0.841970,0.839904]
MaeALS3 = [0.634581,0.635189,0.633433]

print("Mean:")
RMSEMEANALS3 = np.mean(RMSEALS3)
MaeMEANALS3= np.mean(MaeALS3)
print(RMSEMEANALS3)
print(MaeMEANALS3)


print("Standard Deviation:")
RMSEALS3STD =np.std(RMSEALS3)
MaeALS3std = np.std(MaeALS3)
print(RMSEALS3STD)
print(MaeALS3std)

####ALS 4 (Bonus)######
RMSEALS4 = [0.837921,0.839012,0.837543]
MaeALS4 = [0.628807, 0.629753,0.628661]


#Reference: https://pythonforundergradengineers.com/bar-plot-with-error-bars-jupyter-matplotlib.html
#Note the standard deviation bars are small and not visible on the figure
#plt.bar(['ALS 1 RMSE', 'ALS 1 MAE', 'ALS 2 RMSE', 'ALS 2 MAE' ,'ALS 3 RMSE', 'ALS 3 MAE'],[RMSEMEANALS1,MaeMEANALS1,RMSEMEANALS2,MaeMEANALS2,RMSEMEANALS3,MaeMEANALS3 ],yerr=[RMSEALS1STD, MaeALS1std,RMSEAL2STD,MaeALS2std,RMSEALS3STD,MaeALS3std])

#plt.bar(['ALS 1 RMSE', 'ALS 1 MAE', 'ALS 2 RMSE', 'ALS 2 MAE' ,'ALS 3 RMSE', 'ALS 3 MAE'],[RMSEMEANALS1,MaeMEANALS1,RMSEMEANALS2,MaeMEANALS2,RMSEMEANALS3,MaeMEANALS3 ],yerr=[RMSEALS1STD, MaeALS1std,RMSEAL2STD,MaeALS2std,RMSEALS3STD,MaeALS3std])
#plt.bar(['ALS 1 RMSE','ALS 2 RMSE', 'ALS 3 RMSE'],[RMSEMEANALS1,RMSEMEANALS2,RMSEMEANALS3],label = None)
#plt.bar(['ALS 1 MAE','ALS 2 MAE', 'ALS 3 MAE'],[RMSEMEANALS1,RMSEMEANALS2,RMSEMEANALS3], label = None)

plt.bar(['ALS 1 RMSE','ALS 1 MAE'],[RMSEMEANALS1,MaeMEANALS1],yerr=[RMSEALS1STD, MaeALS1std]) #use standard deviation as error bars, however as the std is small it is not visible on the figure
plt.bar(['ALS 2 RMSE','ALS 2 MAE'],[RMSEMEANALS2,MaeMEANALS2],yerr=[RMSEAL2STD,MaeALS2std])
plt.bar(['ALS 3 RMSE','ALS 3 MAE'],[RMSEMEANALS3,MaeMEANALS3],yerr=[RMSEALS3STD,MaeALS3std])

plt.xlabel('ALS strategies and their respective errors', size = 30)
plt.xticks(size = 20)
plt.ylabel('Absolute Error', size = 30)
plt.title('RMSE and MAE values for 3 different ALS strategies', size = 40)


#plt.bar(['ALS 1 RMSE STD','ALS 1 MAE STD'],[RMSEALS1STD, MaeALS1std]) 
#plt.bar(['ALS 2 RMSE STD','ALS 2 MAE STD'],[RMSEAL2STD,MaeALS2std])
#plt.bar(['ALS 3 RMSE STD','ALS 3 MAE STD'],[RMSEALS3STD,MaeALS3std])

#plt.xlabel('ALS strategies and the standard deviation for each error', size = 30)
#plt.xticks(size = 20)
#plt.ylabel('Absolute Error', size = 30)
#plt.title('RMSE and MAE values for 3 different ALS strategies', size = 40)
plt.show()


