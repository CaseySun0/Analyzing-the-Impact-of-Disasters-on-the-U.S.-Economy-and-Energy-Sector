# Import pandas
import pandas as pd
# Import matplot
import matplotlib.pyplot as plt

# Read all csv files into respective dataframes
employment_df = pd.read_csv('unemployment_rate.csv')
cpi_df = pd.read_csv('US_CPI.csv')
disaster_df = pd.read_csv('disasters.csv')
gas_price_df = pd.read_csv('US_gas.csv')
energy_df = pd.read_csv('energy_data.csv')

# Print the info for each data frame to get counts and types
print("\n\nUnemployment Dataset:")
print(employment_df.info())

print("\n\nCPI Dataset:")
print(cpi_df.info())

print("\n\nDisaster Dataset:")
print(disaster_df.info())

print("\n\nGas Dataset:")
print(gas_price_df.info())

print("\n\nEnergy Dataset:")
print(energy_df.info())


## Get MonthYear format for the unemployment dataframe

# convert date column to datetime format
employment_df['date'] = pd.to_datetime(employment_df['date'], format= '%m/%d/%Y')
# create MonthYear column
employment_df['MonthYear'] = employment_df['date'].dt.strftime('%m/%Y')
# make MonthYear column appear first for ease of checking
employment_df= employment_df[['MonthYear'] + [col for col in employment_df.columns if col != 'MonthYear']]
# output to csv to check
employment_df.to_csv('emp_test.csv', index=False)


## Get MonthYear format for the CPI dataframe

# convert the provided date column to datetime
cpi_df['Yearmon'] = pd.to_datetime(cpi_df['Yearmon'], format='%d-%m-%Y')
# create MonthYear column
cpi_df['MonthYear'] = cpi_df['Yearmon'].dt.strftime('%m/%Y')
# make MonthYear column appear first for ease of checking
cpi_df= cpi_df[['MonthYear'] + [col for col in cpi_df.columns if col != 'MonthYear']]
# output to csv to check
cpi_df.to_csv('cpi_test.csv', index=False)


## Get MonthYear format for the disaster dataframe

# convert the provided date column to datetime
disaster_df['declarationDate'] = pd.to_datetime(disaster_df['declarationDate'], format="%Y-%m-%dT%H:%M:%S.%fZ")
# create def function (trying out different methods)
def extract_year_month(date):
    return date.strftime('%m/%Y')
# apply the function to create a new column 'MonthYear'
disaster_df['MonthYear'] = disaster_df['declarationDate'].apply(extract_year_month)
# make MonthYear column appear first for ease of checking
disaster_df= disaster_df[['MonthYear'] + [col for col in disaster_df.columns if col != 'MonthYear']]

# output to csv to check
disaster_df.to_csv('disaster_test.csv', index=False)



## Get MonthYear format for the Gas dataframe

# create def function
def convert_to_datetime(date_string):
    try:
        month, year = date_string.split('-')
        month = pd.to_datetime(month, format='%b').month
        year = int(year)  # Convert year to an integer

        # adjust two-digit year to the correct century (assumes years between 1930 and 2029)
        if year < 30:
            year += 2000
        else:
            year += 1900

        return pd.to_datetime(f"{year}-{month}-01")
    except ValueError:
        return None
    
# convert the "Month" column to datetime objects
gas_price_df['Month'] = gas_price_df['Month'].apply(convert_to_datetime)

# check for missing or invalid date values
invalid_dates = gas_price_df[gas_price_df['Month'].isnull()]
if not invalid_dates.empty:
    print("Invalid dates found:")
    print(invalid_dates)

# create MonthYear column
gas_price_df['MonthYear'] = gas_price_df['Month'].dt.strftime('%m/%Y')
# reorder the columns for ease of checking
gas_price_df = gas_price_df[['MonthYear'] + [col for col in gas_price_df.columns if col != 'MonthYear']]
# output to csv to check
gas_price_df.to_csv('gas_test.csv', index=False)



## Get MonthYear format for the energy dataframe

# Convert the 'Year' column of energy_df to datetime format using the specified format '%Y'
energy_df['Year'] = pd.to_datetime(energy_df['Year'], format='%Y', errors='coerce')
# create MonthYear column
energy_df['MonthYear'] = energy_df['Year'].dt.strftime('%m/%Y')
# reorder the columns for ease of checking
energy_df = energy_df[['MonthYear'] + [col for col in energy_df.columns if col != 'MonthYear']]

# drop missing row
energy_df.drop(energy_df.tail(1).index, inplace=True)
# fill missing values in 'Energy Related CO2missions (Gigatonnes)'
# 'Oil Production (Million barrels per day)','Natural Gas Production (Billion Cubic Metres)',
# 'Coal Production (million tons)' using ffill method
energy_df['Energy Related CO2missions (Gigatonnes)'].fillna(method='ffill', inplace=True)
energy_df['Oil Production (Million barrels per day)'].fillna(method='ffill', inplace=True)
energy_df['Natural Gas Production (Billion Cubic Metres)'].fillna(method='ffill', inplace=True)
energy_df['Coal Production (million tons)'].fillna(method='ffill', inplace=True)

# fill missing values in 'Installed Solar Capacity (GW)' with 0
energy_df['Installed Solar Capacity (GW)'].fillna(0, inplace=True)


# remove ',' from numbers
energy_df['Electricity Generation (Terawatt-hours)'] = energy_df['Electricity Generation (Terawatt-hours)'].str.replace(',', '', regex=True).astype(float)

# create a list of the columns contain values for an entire year
yearly_value_columns = ['Energy Related CO2missions (Gigatonnes)', 'Natural Gas Production (Billion Cubic Metres)', 'Coal Production (million tons)',
                        'Electricity Generation (Terawatt-hours)','Hydroelectricity consumption in TWh', 'Nuclear energy consumption in TWh',
                        'Installed Solar Capacity (GW)','Installed Wind Capacity in GW']

# divide each column by 12 to get monthly values
for column in yearly_value_columns:
    energy_df[column] = energy_df[column] / 12

# output to csv to check
energy_df.to_csv('energy_test.csv', index=False)



### Merge Dataframes ###

# set merged_df to the starting df
merged_df = cpi_df

# create a list of the remaining dataframes set equal to 'dfs_to_merge'
dfs_to_merge = [employment_df, disaster_df, gas_price_df, energy_df]

# create a for loop to iterate through the list and join on "MonthYear" column
# used inner joins so there were no large chunks of missing rows since some csv files contained many years not in others
for df_to_merge in dfs_to_merge:
    merged_df = pd.merge(merged_df, df_to_merge, on='MonthYear', how='inner')

# drop columns
columns_to_drop = ['Yearmon', 'date','declarationDate','fyDeclared','incidentBeginDate',
                   'incidentEndDate','disasterCloseoutDate','hash','id','Month','Year',
                   'fipsCountyCode','placeCode','designatedArea','lastIAFilingDate','lastRefresh']
merged_df.drop(columns=columns_to_drop, inplace=True) 

# drop duplicates
unique_df = merged_df.drop_duplicates()
# output the merged df to a csv 
unique_df.to_csv('merged_data.csv', index=False)


## CPI visualization ##

# create a year column in the data frame
unique_df['Year'] = pd.to_datetime(unique_df['MonthYear']).dt.year

# group the data by year and calculate the mean CPI for each year
unique_df_CPI_grouped = unique_df.groupby('Year')['CPI'].mean().reset_index()

# create a line graph of CPI against the year
plt.figure(figsize=(6, 4))
plt.plot(unique_df_CPI_grouped['Year'], unique_df_CPI_grouped['CPI'])

plt.xlabel('Year')
plt.ylabel('CPI')
plt.title('Average CPI by Year')

plt.show()


## Gas Prices Visualization ##

# create a year column in the data frame
unique_df_gas_grouped = unique_df.groupby('Year')['U.S. All Grades All Formulations Retail Gasoline Prices Dollars per Gallon'].mean().reset_index()
plt.figure(figsize=(6, 4))

# group the data by year and calculate the mean gas price for each year
plt.plot(unique_df_gas_grouped['Year'], unique_df_gas_grouped['U.S. All Grades All Formulations Retail Gasoline Prices Dollars per Gallon'])

plt.xlabel('Year')
plt.ylabel('Gas Prices (in U.S. $)')
plt.title('Average Gas Price by Year')

plt.show()


##  Natural Disaster Visualization ## 

# count how many disaster occurances there were for each month
counts_per_month = unique_df.groupby('MonthYear')['declarationType'].value_counts().unstack().fillna(0)
# plot the counts
counts_per_month.plot(kind='bar', stacked=True)

plt.xlabel('Date')
plt.ylabel('Number of Declared Disasters')
plt.title('January Disaster Counts per Year')
plt.legend(title='Declaration Type')

plt.show()







