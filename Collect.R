library(sparklyr)
spark_available_versions()
spark_install(version="2.4", hadoop_version="2.7") 
options(sparklyr.java9 = TRUE)

conf <- spark_config()
conf$`sparklyr.cores.local` <- 10
conf$`sparklyr.shell.driver-memory` <- "24G"
conf$spark.memory.fraction <- 0.9
sc <- spark_connect(master="local",
                    config = conf)
sc


# Bonus question : Script that download files from Github ------------------

github_patch <- "https://github.com/rvm-courses/GasPrices/raw/master/"

download.price.files <- function(github_patch){
  for (i in 2013:2017){
    zip_path <- paste0(github_patch, "Prix", i, ".zip")
    zip_name <- paste0("Prix", i, ".zip")
    download.file(zip_path, destfile=zip_name)
    unzip(zip_name)
    assign(paste0("prix", i, "_ddf"), spark_read_csv(sc, paste0("Prix", i, ".csv"), header = F, delimiter = ";"))
    system(paste0("rm -r ", zip_name))
  }
}

download.price.files(github_patch)

#Here I download all the price files from 2013 to 2017, then I unzip them, assign a name to each of them and remove the zip file from the folder

# Data Collection ---------------------------------------------------------

# This part is not important if you choose use to use the download function. But if you already have the csv files it will be quicker to process.
prix2013_ddf <- spark_read_csv(sc, path = "Prix2013.csv", header = F, delimiter = ";")
prix2014_ddf <- spark_read_csv(sc, path = "Prix2014.csv", header = F, delimiter = ";")
prix2015_ddf <- spark_read_csv(sc, path = "Prix2015.csv", header = F, delimiter = ";")
prix2016_ddf <- spark_read_csv(sc, path = "Prix2016.csv", header = F, delimiter = ";")
prix2017_ddf <- spark_read_csv(sc, path = "Prix2017.csv", header = F, delimiter = ";")

# This part import gas Stations file & Services file (2017 versions) as Spark data table.
Services2017_ddf <- spark_read_csv(sc, path = "Services2017.csv", header = F, delimiter = ";")
Stations2017_ddf <- spark_read_csv(sc, path = "Stations2017.csv", header = F, delimiter = ";")

# Merge the ddf together --------------------------------------------------

# Here I choose to row binds all the price table together.
prixTot_ddf <- sdf_bind_rows(prix2013_ddf, prix2014_ddf, prix2015_ddf, prix2016_ddf, prix2017_ddf)

# And I add names to the prixTot_ddf distributed dataframe.
names(prixTot_ddf) <- c("id_pdv",
                         "cp",
                         "pop",
                         "latitude",
                         "longitude",
                         "date",
                         "id_carburant",
                         "nom_carburant",
                         "prix") 

prixTot_ddf %>% dplyr::count()
#We observe that we optain 15215738 rows on our ddf.

# Split date in year, month, week of the year -----------------------------

#Importing the librarys tidy verse for the use of dyplr mainly and lubridate to manage the dates
library(tidyverse)
library(lubridate)

#Here I add the columns year, month and day thancks to the corresponding lubridate's functions
prixTot_ddf <- prixTot_ddf %>% 
  dplyr::mutate(year = year(date), 
                 month = month(date), 
                 day = day(date))

#Sadly the week function of the lubridate package is not working with spark because there is not equivqlent SQL queries, to to avoid this problem
#I choose to build an aproximation of the week number by assuming that every month is made of 30 days. I also add the column day_year which count 
#the number of the day in the year.
prixTot_ddf <- prixTot_ddf %>% 
  dplyr::mutate(day_year = (30*(month -1) + day),
                day_year_tot = (360*(year - 2013) + day_year),
                week = floor((30*(month -1) + day)/7) + 1)
 

# Prepare latitude & longitude for mapping --------------------------------

prixTot_ddf <- prixTot_ddf %>%
  dplyr::mutate(latitude_new =  latitude/100000 , 
                longitude_new = longitude/100000) %>% 
  dplyr::select(-c('latitude', 'longitude'))

head(prixTot_ddf, 5)

# Make data available as a table in order to be able to use Spark  --------

sdf_register(prixTot_ddf, "prixTot_ddf")

# Compute price index for each station per week: --------------------------

# Here I add the column Average_Day_Price_in_France ad price_index thancks to the formula on the slide.
prixTot_ddf <- prixTot_ddf %>%
  dplyr::group_by(id_carburant, day_year) %>% 
  dplyr::mutate(Average_Day_Price_in_France = mean(prix)) %>% dplyr::ungroup() %>%
  dplyr::mutate(price_index = 100 * ((prix - Average_Day_Price_in_France)/(Average_Day_Price_in_France))+1)%>% 
  dplyr::select(-c('Average_Day_Price_in_France'))

# Compute week index: -----------------------------------------------------

#Here the week index has been made with the same idea as the day_year variable.
prixTot_ddf <- prixTot_ddf %>%
  dplyr::mutate(week_index = (52*(year - 2013) + week))

#Here is an exemple for years 2013 to 2017, 4*52=208 and as you can see wee obtain 209 
prixTot_ddf %>% dplyr::filter(year == 2017, week == 2)

# Data Visualization ------------------------------------------------------
library(ggplot2)
library(hrbrthemes)

#Here is the plot representing the weekly evolution of average gas price over France
prixTot_ddf %>%
  dplyr::group_by(week_index, nom_carburant) %>%
  dplyr::summarise(average_price = mean(prix)) %>% dplyr::ungroup() %>%
  ggplot( aes(x=week_index, y= average_price, group=nom_carburant, color=nom_carburant)) +
  theme_ipsum()+
  geom_line() +
  theme(
    plot.title = element_text(size=10)
  )+
  ggtitle("Average price of gas for each week from 2014 to 2017 ") +
  ylab("Average gas price (in thunsands of euros)")+
  xlab("Week index")

#Bonus plot, representation in a grid of each gas type of France geo heat maps of price indexes at department level
prixTot_ddf %>%
  dplyr::mutate(departement = substr(cp, start = 1, stop = 2)) %>% 
  dplyr::group_by(departement, nom_carburant) %>%
  dplyr::mutate(mean_price_index = mean(price_index)) %>%  dplyr::ungroup() %>%
  dplyr::select(departement, mean_price_index, nom_carburant) %>% 
  distinct() %>% na.omit() %>%
  ggplot(aes(x=departement, y=nom_carburant, fill=mean_price_index)) + 
  geom_tile() +
  ggtitle("heat maps of price indexes") +
  theme(plot.title = element_text(hjust = 0.5))

#Here we observe that the E85 is way more expensive in the departement 48 (Lozere), and less in the 
#deparement 50 (La Manche). 

# Forecast next day price -------------------------------------------------

#Before doeing the forecast I remove the potential NA of the data because sparklyr ml functions seems not to be working with NAs.
prixTot_ddf <- na.omit(prixTot_ddf)

prixTot_ddf <- prixTot_ddf %>% 
  dplyr::mutate(day_year_tot = (360*(year - 2013) + day_year))

#This was one of the multiple try to create a lag price function, sparklyr makes it pretty difficult.
#prixTot_ddf <- prixTot_ddf %>% 
  #dplyr::mutate(lag_price = lag(prix, n = 1L))
 
partitions <- prixTot_ddf %>%
  sdf_random_split(training = 0.7, test = 0.3, seed = 1111)

gas_train <- partitions$training
gas_train  %>% dplyr::count()
gas_test <- partitions$test

#Here is the linear regression of price thancks to the day, the station id and the type of gas
lm_model <- gas_train %>% ml_linear_regression(response = "prix", features = c("day_year_tot", "id_pdv", "nom_carburant"))

summary(lm_model)
lm_pred <- ml_predict(lm_model, gas_test)

ml_regression_evaluator(lm_pred, label_col = "prix")

#Here is the random forest regression of price thancks to the day, the station id and the type of gas
rf_model <- gas_train %>% ml_random_forest_regressor(response = "prix", features = c("day_year_tot", "id_pdv", "nom_carburant"))
summary(rf_model)
rf_pred <- ml_predict(rf_model, gas_test)

ml_regression_evaluator(rf_pred, label_col = "prix")

# Disconect spark ---------------------------------------------------------

spark_disconnect(sc)
