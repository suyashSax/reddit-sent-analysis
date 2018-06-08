# May first need:
# In your VM: sudo apt-get install libgeos-dev (brew install on Mac)
# pip3 install https://github.com/matplotlib/basemap/archive/v1.1.0.tar.gz

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import numpy as np

import random as r

from mpl_toolkits.basemap import Basemap as Basemap
from matplotlib.colors import rgb2hex
from matplotlib.patches import Polygon

"""
IMPORTANT
This is EXAMPLE code.
There are a few things missing:
1) You may need to play with the colors in the US map.
2) This code assumes you are running in Jupyter Notebook or on your own system.
   If you are using the VM, you will instead need to play with writing the images
   to PNG files with decent margins and sizes.
3) The US map only has code for the Positive case. I leave the negative case to you.
4) Alaska and Hawaii got dropped off the map, but it's late, and I want you to have this
   code. So, if you can fix Hawaii and Alask, ExTrA CrEdIt. The source contains info
   about adding them back.
"""


"""
PLOT 1: SENTIMENT OVER TIME (TIME SERIES PLOT)
"""
# Assumes a file called time_data.csv that has columns
# date, Positive, Negative. Use absolute path.

#djt
#ts = pd.read_csv("time_data.csv")
#dem
ts = pd.read_csv("time_data_dem.csv")
#gop
#ts = pd.read_csv("time_data_gop.csv")

# Remove erroneous row.
ts = ts[ts['date'] != '2018-12-31']

plt.figure(figsize=(12,5))
ts.date = pd.to_datetime(ts['date'], format='%Y-%m-%d')
ts.set_index(['date'],inplace=True)

#djt
#ax1 = ts.plot(title="President Trump Sentiment on /r/politics Over Time", color=['green', 'red'], ylim=(0, 1.05))

#dem
ax1 = ts.plot(title="Democratic Sentiment on /r/politics Over Time", color=['green', 'red'], ylim=(0, 1.05))

#gop
#ax1 = ts.plot(title="GOP Sentiment on /r/politics Over Time", color=['green', 'red'], ylim=(0, 1.05))

ax1.plot()

#djt
#plt.savefig("part1.png")
#dem
plt.savefig("part1_dem.png")
#gop
#plt.savefig("part1_gop.png")

plt.clf()

"""
PLOT 2: SENTIMENT BY STATE (POSITIVE AND NEGATIVE SEPARATELY)
# This example only shows positive, I will leave negative to you.
"""

# This assumes you have a CSV file called "state_data.csv" with the columns:
# state, Positive, Negative
#
# You should use the FULL PATH to the file, just in case.

#djt
#state_data = pd.read_csv("state_data.csv")
#dem
state_data = pd.read_csv("state_data_dem.csv")
#gop
#state_data = pd.read_csv("state_data_gop.csv")

"""
You also need to download the following files. Put them somewhere convenient:
https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shp
https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.dbf
https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shx
"""

# Lambert Conformal map of lower 48 states.
m = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
        projection='lcc', lat_1=33, lat_2=45, lon_0=-95)
shp_info = m.readshapefile('st99_d00','states',drawbounds=True)  # No extension specified in path here.
pos_data = dict(zip(state_data.state, state_data.Positive))
# choose a color for each state based on sentiment.
pos_colors = {}
statenames = []
pos_cmap = plt.cm.Greens # use 'Greens' colormap

vmin = 34.6177474403; vmax = 19 # set range.
for shapedict in m.states_info:
    statename = shapedict['NAME']
    # skip DC and Puerto Rico.
    if statename not in ['District of Columbia', 'Puerto Rico']:
        pos = pos_data[statename]
        pos_colors[statename] = pos_cmap(1. - np.sqrt(( pos - vmin )/( vmax - vmin)))[:3]
    statenames.append(statename)
# cycle through state names, color each one.
#
# POSITIVE MAP
ax2 = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        if statenames[nshape] == 'Alaska':
            seg = list(map(lambda (x,y): (0.30*x + 1100000, 0.30*y-1300000), seg))
        if statenames[nshape] == 'Hawaii':
            seg = list(map(lambda (x,y): (x + 5100000, y-900000), seg))
        color = rgb2hex(pos_colors[statenames[nshape]])
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax2.add_patch(poly)

#djt
#plt.title('Positive Trump Sentiment Across the US')
#dem
plt.title('Positive Democratic Sentiment Across the US')
#gop
#plt.title('Positive GOP Sentiment Across the US')

plt.xlabel('')
plt.ylabel('')
#plt.xkcd()

#djt
#plt.savefig("pos_map.png")
#dem
plt.savefig("pos_map_dem.png")
#gop
#plt.savefig("pos_map_gop.png")


plt.clf()

m = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
        projection='lcc', lat_1=33, lat_2=45, lon_0=-95)
shp_info = m.readshapefile('st99_d00','states',drawbounds=True)  # No extension specified in path here.
neg_data = dict(zip(state_data.state, state_data.Negative))

neg_colors = {}
statenames = []
neg_cmap = plt.cm.hot # use 'hot' colormap

vmin = 83.025; vmax = 91.728 # set range.
for shapedict in m.states_info:
    statename = shapedict['NAME']
    # skip DC and Puerto Rico.
    if statename not in ['District of Columbia', 'Puerto Rico']:
        neg = neg_data[statename]
        neg_colors[statename] = neg_cmap(1. - np.sqrt(( neg - vmin )/( vmax - vmin)))[:3]
    statenames.append(statename)
# cycle through state names, color each one.

# NEGATIVE MAP
ax3 = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        color = rgb2hex(neg_colors[statenames[nshape]])
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax3.add_patch(poly)

#djt
#plt.title('Negative Trump Sentiment Across the US')
#dem
plt.title('Negative Democratic Sentiment Across the US')
#gop
#plt.title('Negative GOP Sentiment Across the US')

plt.xlabel('')
plt.ylabel('')

#djt
#plt.savefig("neg_map.png")
#dem
plt.savefig("neg_map_dem.png")
#gop
#plt.savefig("neg_map_gop.png")


plt.clf()

#DIFFERENCE MAP
m = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
        projection='lcc', lat_1=33, lat_2=45, lon_0=-95)
shp_info = m.readshapefile('st99_d00','states',drawbounds=True)  # No extension specified in path here.

#djt
#diff_state_data = pd.read_csv("difference.csv")
#dem
diff_state_data = pd.read_csv("difference_dem.csv")
#gop
#diff_state_data = pd.read_csv("difference_gop.csv")



diff_data = dict(zip(diff_state_data.state, diff_state_data.Difference))

diff_colors = {}
statenames = []
diff_cmap = plt.cm.Purples # use 'Purples' colormap

vmin = 54.91; vmax = 70.37 # set range.
for shapedict in m.states_info:
    statename = shapedict['NAME']
    # skip DC and Puerto Rico.
    if statename not in ['District of Columbia', 'Puerto Rico']:
        diff = abs(diff_data[statename])
        diff_colors[statename] = diff_cmap(1. - np.sqrt(( diff - vmin )/( vmax - vmin)))[:3]
    statenames.append(statename)
# cycle through state names, color each one.

ax4 = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        color = rgb2hex(diff_colors[statenames[nshape]])
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax4.add_patch(poly)
#djt
#plt.title('Difference (POS-NEG) Trump Sentiment Across the US')
#dem
plt.title('Difference (POS-NEG) Democratic Sentiment Across the US')
#gop
#plt.title('Difference (POS-NEG) GOP Sentiment Across the US')

plt.xlabel('')
plt.ylabel('')

#djt
#plt.savefig("diff_map.png")
#dem
plt.savefig("diff_map_dem.png")
#gop
#plt.savefig("diff_map_gop.png")


plt.clf()

# SOURCE: https://stackoverflow.com/questions/39742305/how-to-use-basemap-python-to-plot-us-with-50-states
# (this misses Alaska and Hawaii. If you can get them to work, EXTRA CREDIT)

"""
PART 4 SHOULD BE DONE IN SPARK
"""

"""
PLOT 5A: SENTIMENT BY STORY SCORE
"""
# What is the purpose of this? It helps us determine if the story score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called submission_score.csv with the following coluns
# submission_score, Positive, Negative

#djt
#story = pd.read_csv("submission_score.csv")
#dem
story = pd.read_csv("story_score_dem.csv")
#gop
#story = pd.read_csv("story_score_gop.csv")

plt.figure(figsize=(12,5))
fig = plt.figure()
ax5 = fig.add_subplot(111)

ax5.scatter(story['submission_score'], story['Positive'], s=10, c='b', marker="s", label='Positive')
ax5.scatter(story['submission_score'], story['Negative'], s=10, c='r', marker="o", label='Negative')
plt.legend(loc='lower right');

#djt
#plt.xlabel('President Trump Sentiment by Submission Score')
#dem
plt.xlabel('Democratic Sentiment by Submission Score')
#gop
#plt.xlabel('GOP Sentiment by Submission Score')

plt.ylabel("Percent Sentiment")
plt.show()

#djt
#plt.savefig("plot5a.png")
#dem
plt.savefig("plot5a_dem.png")
#gop
#plt.savefig("plot5a_gop.png")

plt.clf()

"""
PLOT 5B: SENTIMENT BY COMMENT SCORE
"""
# What is the purpose of this? It helps us determine if the comment score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called comment_score.csv with the following columns
# comment_score, Positive, Negative

#djt
#story = pd.read_csv("comment_score.csv")
#dem
story = pd.read_csv("comment_score_dem.csv")
#gop
#story = pd.read_csv("comment_score_gop.csv")

plt.figure(figsize=(12,5))
fig = plt.figure()
ax6 = fig.add_subplot(111)

ax6.scatter(story['comment_score'], story['Positive'], s=10, c='b', marker="s", label='Positive')
ax6.scatter(story['comment_score'], story['Negative'], s=10, c='r', marker="o", label='Negative')
plt.legend(loc='lower right');

#djt
#plt.xlabel('President Trump Sentiment by Comment Score')
#dem
plt.xlabel('Democratic Sentiment by Comment Score')
#gop
#plt.xlabel('GOP Sentiment by Comment Score')

plt.ylabel("Percent Sentiment")

#djt
#plt.savefig("plot5b.png")
#dem
plt.savefig("plot5b_dem.png")
#gop
#plt.savefig("plot5b_gop.png")

plt.clf()
