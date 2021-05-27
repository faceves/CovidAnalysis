# Instructions:
- Create a Spark Application that process Covid data.

- Your project  should involve some analysis of covid data(Every concept of spark from rdd, dataframes, sql, dataset and 
optimization methods should be included, persistence also). The final expected output is different trends that you have
observed as part of data collectivley and how can WHO make use of these trends to make some useful decisions.

- Lets the P2 Demo, have presentation with screen shots and practical demo for at least one of your trends.

## Datasets:
Columns and our understanding of them.

covid_19_data.csv
------------------------------------------------------------------
    SNo                             ?
    ObservationDate                 " "
    Province/State                  " "                  
    Country/Region                  " "
    Last Update                     " "                        
    Confirmed                       " "
    Deaths                          " "
    Recovered                       " "

time_series_covid_19_confirmed_US.csv
------------------------------------------------------------------
    UID                                 Unique ID = code3 + FIPS
    iso2                                Country code w/2 letters            
    iso3                                Country code w/3 letters
    code3                               3 Digit Country Code                             
    FIPS                                Federal Information Processing Standard/County Code
    Admin2                              City
    Province_State                      " "
    Country_Region                      " "
    Lat                                 " "
    Long_                               " "
    Combined_Key                        City + Province/State + Country/Region
    Dates ...                           1/22/20 to 5/2/21


time_series_covid_19_deaths_US.csv
------------------------------------------------------------------
    Same as time_series_covid_19_confirmed_US.csv buit with an extra column:
    Population                          " "


time_series_covid_19_confirmed.csv,  time_series_covid_19_deaths.csv,  time_series_covid_19_recovered.csv
------------------------------------------------------------------
    Province/State                      " "
    Country/Region                      " "
    Lat                                 " "
    Long                                " "
    Dates                               1/22/20 to 5/2/21
   

## Trend Brainstorming:
- Geographical Location:
    - Urban vs Rural vs Remote
- Important Dates at Geographical Locations:
    - Holidays:
        - Mardi Gras
        - 4th of July
        - Memorial Day
        - Christmas
        - Hannukah
        - Easter
        - Ramadan
- Seasons at Geographical Location:
    - Spring
    - Summer
    - Fall
    - Winter

- External Knowledge/Data need to know:
    - Policy Changes according to Location
        - Mask Mandates
    - Geographical Popularity during Seasons
    
## Trends:
- Causes that affect Outcomes
    1. U.S vs Countries
       1. Season Spikes
       2. Holiday Spikes
    2. U.S State vs State
       1. Season Spikes
       2. Holiday Spikes
    3. Continents
    4. Urban vs Rural
    5. First Occurrence
       1. Compare to neighboring First Occurrences
    6. First Spike
       1. Compare to neighboring First Spikes
    7. Length of Spike
        1. Compare Length of Spikes to Others
    8. Something with Lat & Long?