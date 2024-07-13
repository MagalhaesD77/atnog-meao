import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import textwrap

# Reading data from a CSV file
file_path = 'resultsv2.csv'  # replace with your CSV file path
df = pd.read_csv(file_path)

new_labels = [
    'Time until Metrics Reception',
    'Time until Migration Decision',
    'Time until Target Node Pod Initialization',
    'Time until Target Node Pod Ready',
    'Time until Original Node Pod Termination',
    'Time until Migration Completion in OSM'
]

df['Time until Metrics Reception'] = (
    df['Metrics Collection']
    + df['Metrics Reception']
)

df['Time until Migration Decision'] = (
    df['Time until Metrics Reception']
    + df['Migration Decision']
)

df['Time until Target Node Pod Initialization'] = (
    df['Time until Migration Decision']
    + df['Target Node Pod Initialization']
)

df["Time until Target Node Pod Ready"] = (
    df['Time until Target Node Pod Initialization']
    + df["Target Node Pod Ready"]
)

df["Time until Original Node Pod Termination"] = (
    df["Time until Target Node Pod Ready"]
    + df["Original Node Pod Termination"]
)

df["Time until Migration Completion in OSM"] = (
    df["Time until Original Node Pod Termination"]
    + df["Migration Completion in OSM"]
)

# Drop the "Metrics collected" column and convert to seconds
df = df.drop(columns=["Metrics Collection","Metrics Reception","Migration Decision","Target Node Pod Initialization","Target Node Pod Ready","Original Node Pod Termination","Migration Completion in OSM"]) / 1000

# Melt the DataFrame to format it for Seaborn
df_melted = pd.melt(df, var_name='Migration Phase', value_name='Time (s)')

# Create the boxplot
plt.figure(figsize=(18, 9))
sns.boxplot(x='Migration Phase', y='Time (s)', data=df_melted)

# Add grid lines for better readability
plt.grid(axis='y', which='both', linestyle='-', linewidth=0.5, color='lightgrey')  # minor and major grid lines

# Customize y-axis ticks and grid lines
plt.gca().yaxis.set_major_locator(MultipleLocator(5))  # major grid lines every 5 units
plt.gca().yaxis.set_minor_locator(MultipleLocator(1))  # minor grid lines every 1 unit

# Differentiate grid lines for major and minor ticks
plt.gca().yaxis.grid(True, which='major', linestyle='-', linewidth=0.8, color='lightgrey')
plt.gca().yaxis.grid(True, which='minor', linestyle='--', linewidth=0.5, color='lightgrey')

# Adjust ylim to start from 0
plt.ylim(bottom=0)

# Function to wrap labels
def wrap_labels(ax, width):
    labels = []
    for label in ax.get_xticklabels():
        text = label.get_text()
        wrapped_text = "\n".join(textwrap.wrap(text, width))
        labels.append(wrapped_text)
    ax.set_xticklabels(labels, rotation=0, ha='center')

plt.gca().set_xticklabels(new_labels, rotation=0, ha='center')
wrap_labels(plt.gca(), 15)

# Rotate x-axis tick labels to horizontal
plt.xticks(rotation=0)

plt.title('Duration of Migration Phases', fontsize=16)
plt.xlabel('Migration Phase', fontsize=14)
plt.ylabel('Time (s)', fontsize=14)

plt.tight_layout()
plt.show()