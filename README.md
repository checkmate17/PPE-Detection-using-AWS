## capstone_ppe_detection_dash_dashboard

# Dashboard details:
This is the plotly dash code for - PPE detection dashboard created from  simulated data. Following are details of the implementation:
1. modified_capstone_simulation.ipynb : This file has the code for generating simulated non-compliant cases for 100 companies over 12 months. The output of this program is the simulated data stored in a "simulation_dash.csv" csv file.

2. app-with-tabs.ipynb : This file has all the plotly dash code for the dashboard. Please see the explanation of each cell below:
  Cell 1: This cell has the code for importing all the dependencies (libraries). Also, has to code for creating the data dataframe from the simulated data csv.
  Cell 2: Contains two callback functions : 
              1. First callback function is for displaying the correlation between the non-compliant cases detected and claims submitted for a particular company selected from the "Select Company :" dropdown.
              2. Second callback function is for populating tab-1 "Trend" and tab-2 "Live".
  Cell 3: Code to initialize the app server, define styling for tab-1 and tab-2 and code to define the layout of the dashboard application
  Cell 4: Code to Run the application.
