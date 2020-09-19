# PPE Detection Using AWS

## Dashboard details:
This is the plotly dash code for - PPE detection dashboard created from  simulated data. Following are details of the implementation:

"modified_capstone_simulation.ipynb" : This file has the code for generating simulated non-compliant cases for 100 companies over 12 months. The output of this program is the simulated data stored in a "simulation_dash.csv" csv file.

"app-with-tabs.ipynb" : This file has all the plotly dash code for the dashboard. Please see the explanation of each cell below:

  Cell 1: This cell has the code for importing all the dependencies (libraries). Also, has to code for creating the data dataframe from the simulated data csv.
  
  Cell 2: Contains two callback functions : 
              First callback function is for displaying the correlation between the non-compliant cases detected and claims submitted 
              for a particular company selected from the "Select Company :" dropdown.
              Second callback function is for populating tab-1 "Trend" and tab-2 "Live".
              
  Cell 3: Code to initialize the app server, define styling for tab-1 and tab-2 and code to define the layout of the dashboard application
  
  Cell 4: Code to Run the application.
  
  
## Model Details:
  
The AWS folder contains the code we have adapted for both the DeepLens and cloud-based lambda functions. For the latter, we included scripts for 2 main use cases: 

1. Pretrained model for labels that already exist in Rekognition's catalogue (helmets and boots)
2. Pretrained model + Custom Labels model that uses both Rekognition's catalogue and labels we have trained ourselves (masks and vests) 

To run the second one, you are required to have your own custom labels and obtain an ARN to call the resource. For more details on how to call an API, please refer to this site: https://docs.aws.amazon.com/rekognition/latest/customlabels-dg/ex-lambda.html 

You are also required to install Amazon's CLI in your computer: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

Please note that Custom Labels are charged by the hour, so make sure to stop the resource to avoid incurring in additional charges.
