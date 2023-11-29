## Assess data
Contains code for assessing data quality

### Intrinsic quality
How reliable the data is, defined by 4 main metrics
- Integrity
- Completeness
- Consistency
- Accuracy

### Contextual quality (work in progress)
How "good" the data is for a specific task. 
Some metrics to consider:
- Ratio between number of children in registry and number of children in other external datasets
- Data density across a timeframe, in each region

Contextual quality should only be done after the data cleaning steps

### Files
- **3_1_intrinsic_quality.Rmd** - perform intrinsic quality assessment
- **3_2_visualize_data.Rmd** - source code for graphs
- **3_3_contextual_quality.Rmd** (to-do)