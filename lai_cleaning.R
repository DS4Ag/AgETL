library(dplyr)
library(magrittr)

setwd('C:/Users/luizv/OneDrive_Purdue/OneDrive - purdue.edu/CIMMYT-Purdue RS&CropModelling/Data workflow/LAI_data_process')

# read lai dataframe
dataFrame <- read.csv('./output/LAI_ACRE_2022.csv')

# delete lai values < 0
dataFrame <- dataFrame %>% filter(lai > 0) 

# transform date values from int to date format
dataFrame$date <- as.character(dataFrame$date)
dataFrame %<>% mutate(date= as.Date(date, format= '%Y%m%d'))

#Export file
output_dir = 'input_database'
if (!dir.exists(output_dir)) {dir.create(output_dir)}
write.csv(dataFrame, './input_database/LAI_ACRE_2022.csv', row.names = FALSE)




