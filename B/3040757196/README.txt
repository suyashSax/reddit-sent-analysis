Extra Credit: 

We got Hawaii and Alaska to show up on all of our maps.

We attempted to analyze the GOP and DEM columns from the json in addition to the djt data. 
We created two extra data frames for this data and created the corresponding graphs.

We also analyzed author flairs that were “foreign” and plotted them against time to see
if they posts from foreign sources had any spikes or dips corresponding to the investigation of outside interference in the 2016 US election.

Other:

1. Graphs are created using a 20% sample of the dataset, with 2-fold cross validation.
2. Datasets were moved out of the www folder into the directory of the shell. As such, data is read from './dataset' rather than '../www/dataset' 
3. We used the iPython shell, in the off chance that reddit_model.py does not compile - please copy-paste into an iPython shell.
4. CSVs are generated with different names each time, so the names must be changed in 
analysis.py before running on a new batch of CSVs.