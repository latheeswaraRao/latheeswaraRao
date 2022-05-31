import pandas as pd
import matplotlib.pylab as plt
import numpy as np


class deviation():   
    '''
    Importing the train and ideal files as in pandas dataframes
    To find the deviations required to plot the graphs
    '''
    def __init__(self, train, ideal):

        self.train = train
        self.ideal = ideal
        self.x = self.train.loc[:, ['x']]
            
    def Initializer(self):
        df = pd.DataFrame( )
        for i in range(1,number_of_columns_of_traning):
            lst = self.finding_deviations(i)
            df[i] = np.array(lst)
        return df
    
    def finding_deviations(self, num):
        y1=  self.train.iloc[:, num]
        list = [ ]
        for i in range(1,number_of_columns_of_ideal):
            y2= self.ideal.iloc[:, i]
            list.append(self.difference(y1,y2))
        return list
        
    def difference(self, y1,y2):
        deviation = 0
        for i in range(number_of_rows):
            deviation = deviation + (y1[i]-y2[i])**2
        return deviation
    

class Graphs():

    def __init__(self, len ,Ideal_function, train, ideal):
        '''
        Importing the values of train, ideal,len and Ideal_function
        '''
        self.len = len
        self.ideal_function = Ideal_function
        self.train = train
        self.ideal = ideal
        self.x = self.train.loc[:, ['x']]

    def Downwards(self):
        '''
        To Plot them downwards
        And saves thwm in their respective names
        '''
        for i in range(0,self.len):
            y = self.ideal.loc[:, [self.ideal_function[i]]]  
            y1 = self.train.iloc[:,i+1] 

            plt.figure(self.ideal_function[i])
            plt.title( "Ideal Function - "+ self.ideal_function[i], 
                                                fontsize=20, color ="red")
            #plt.plot(x,y,"r",x,y1,"b",)    To plot in the same axis or graph

            plt.subplot(2,1,1)
            plt.plot(x,y,"r")

            plt.subplot(2,1,2)
            plt.plot(x,y1,"b")

            plt.savefig(" Downwards--" + self.ideal_function[i] + " .jpg")


    def Side_By_Side(self):
        '''
        To plot on SIde BY side
        FOR GRAPHS TO APPEAR ON SIDE WARDS but as smaller
        '''
        for i in range(0,len):
            y = ideal.loc[:, [self.ideal_function[i]]]  
            y1 = train.iloc[:,i+1]
            plt.figure()
            plt.subplot(2,2,1)
        
            plt.plot(x,y,"r")
            plt.title( "Ideal Function - "+ self.ideal_function[i],
                                                        fontsize=10, color ="red")

            plt.subplot(2,2,2)
        
            plt.plot(x,y1,"b")
            plt.title( "Training Function - "+ self.ideal_function.index[i],
                                                         fontsize=10, color ="red")

            plt.savefig(" Side_By_Side--" + self.ideal_function[i]+ " .jpg")
        

    def Best_Fit(self):
        '''
        To Plot the graph side ways with proper conventions
        '''

        for i in range(0,len):
            y = ideal.loc[:, [self.ideal_function[i]]]  
            y1 = train.iloc[:,i+1]
            
            fig = plt.figure(figsize=(15,5))
            ax1 = fig.add_subplot(1,2,1)
            ax2 = fig.add_subplot(1,2,2)

            ax1.plot(x,y1)
            ax1.set_title( "Training Function - "+ self.ideal_function.index[i],
                                                    fontsize= 20, color ="red")
            
            ax2.plot(x,y)
            ax2.set_title( "Ideal Function - "+ self.ideal_function[i],
                                                    fontsize=20, color ="red")
            
            plt.savefig("Best_Fit--" + self.ideal_function[i] + " .jpg" )


if __name__ =="__main__":

    train = pd.read_csv("train.csv")
    ideal = pd.read_csv("ideal.csv")

    number_of_columns_of_traning = train.shape[1]
    number_of_columns_of_ideal = ideal.shape[1]
    number_of_rows = train.shape[0]
    len1 = number_of_columns_of_traning-1
    len2 = number_of_columns_of_ideal-1
    len = number_of_columns_of_traning-1

    x = train.loc[:,'x']

    deviation_object = deviation(train, ideal)
    df = deviation_object.Initializer()

    cols = train.columns[1:]
    indexes = ideal.columns[1:]
    df.columns = cols
    df.index = indexes
    ideal_function = df.idxmin()
    ideal_function= pd.DataFrame(ideal_function)
    ideal_function =  ideal_function.iloc[:,0]
   

    graph_object = Graphs(len, ideal_function, train, ideal)
    graph_object.Best_Fit()
    graph_object.Side_By_Side()
    graph_object.Downwards()
   
   