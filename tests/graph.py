import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import textwrap
from matplotlib import gridspec
import numpy as np
import scipy.stats as st 

# Reading data from a CSV file
file_path = 'resultsv2.csv'  # replace with your CSV file path
df = pd.read_csv(file_path)

# Drop the "Metrics Collection" column and convert to seconds
df = df.drop(columns=["Metrics Collection"]) / 1000

# create 99% confidence interval
for key, data in df.items(): 
    print(len(data))
    confidence = st.t.interval(confidence=0.99, 
                df=len(data)-1, 
                loc=np.mean(data),  
                scale=st.sem(data)) 
    print(confidence)
    newdata = []
    for d in data:
        if d > confidence[0] and d < confidence[1]:
            newdata.append(d)
    print(len(newdata))

# Calculate additional metrics
df["Total Time until Target Node Pod Ready"] = (
    df["Metrics Reception"]
    + df["Migration Decision"]
    + df["Target Node Pod Initialization"]
    + df["Target Node Pod Ready"]
)

df["Total Time until Source Node Pod Termination"] = (
    df["Total Time until Target Node Pod Ready"]
    + df["Source Node Pod Termination"]
)

df["Total Time until Migration Completion in OSM"] = (
    df["Total Time until Source Node Pod Termination"]
    + df["Migration Completion in OSM"]
)

# Melt the DataFrame to format it for Seaborn
df_melted = pd.melt(df, var_name='Migration Phase', value_name='Time (s)')

# Create the boxplot with broken axis
fig = plt.figure(figsize=(18, 9))
gs = gridspec.GridSpec(2, 1, height_ratios=[5, 1])  # create gridspec with larger scale above

# Create the first subplot for larger values
ax1 = plt.subplot(gs[0])
sns.boxplot(x='Migration Phase', y='Time (s)', data=df_melted, ax=ax1)
ax1.set_ylim(0.5, df_melted['Time (s)'].max())
ax1.spines['bottom'].set_visible(False)
ax1.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)

# Create the second subplot for smaller values
ax2 = plt.subplot(gs[1])
sns.boxplot(x='Migration Phase', y='Time (s)', data=df_melted, ax=ax2)
ax2.set_ylim(0, 0.04)  # Adjust scale for smaller values
ax2.spines['top'].set_visible(False)

# Add diagonal lines to indicate the broken axis
d = .0075  # how big to make the diagonal lines in axes coordinates
kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
ax1.plot((-d, +d), (-d, +d), **kwargs)
ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)

kwargs.update(transform=ax2.transAxes)
ax2.plot((-d, +d), (1.10 - 8*d, 1.10 + d), **kwargs)
ax2.plot((1 - d, 1 + d), (1.10 - 8*d, 1.10 + d), **kwargs)

# Add grid lines for better readability
ax1.grid(axis='y', which='both', linestyle='-', linewidth=0.5, color='lightgrey')  # minor and major grid lines
ax2.grid(axis='y', which='both', linestyle='-', linewidth=0.5, color='lightgrey')  # minor and major grid lines

# Customize y-axis ticks and grid lines
ax1.yaxis.set_major_locator(MultipleLocator(5))  # major grid lines every 5 units for larger values
ax1.yaxis.set_minor_locator(MultipleLocator(1))  # minor grid lines every 1 unit for larger values
ax2.yaxis.set_major_locator(MultipleLocator(0.01))  # major grid lines every 0.01 units for smaller values
ax2.yaxis.set_minor_locator(MultipleLocator(0.005))  # minor grid lines every 0.005 units for smaller values

# Differentiate grid lines for major and minor ticks
ax1.yaxis.grid(True, which='major', linestyle='-', linewidth=0.8, color='grey')
ax1.yaxis.grid(True, which='minor', linestyle='--', linewidth=0.5, color='lightgrey')
ax2.yaxis.grid(True, which='major', linestyle='-', linewidth=0.8, color='grey')
ax2.yaxis.grid(True, which='minor', linestyle='--', linewidth=0.5, color='lightgrey')

# Function to wrap labels
def wrap_labels(ax, width):
    labels = []
    for label in ax.get_xticklabels():
        text = label.get_text()
        wrapped_text = "\n".join(textwrap.wrap(text, width))
        labels.append(wrapped_text)
    ax.set_xticklabels(labels, rotation=0, ha='center')

# New labels
new_labels = [
    'Metrics Reception',
    'Migration Decision',
    'Target Node Pod Initialization',
    'Target Node Pod Ready',
    'Source Node Pod Termination',
    'Migration Completion in OSM',
    'Total Time until Target Node Pod Ready',
    'Total Time until Source Node Pod Termination',
    'Total Time until Migration Completion in OSM'
]

# Apply new labels
wrap_labels(ax2, 15)

# Title and labels
ax1.set_title('Duration of Migration Phases', fontsize=16)
ax1.set_xlabel('')
ax2.set_xlabel('Migration Phase', fontsize=14)
ax1.set_ylabel('Time (s)', fontsize=14)
ax2.set_ylabel('Time (s)', fontsize=14)

plt.tight_layout()

plt.savefig('migration_phases_boxplot.png', format='png', dpi=300)

plt.show()