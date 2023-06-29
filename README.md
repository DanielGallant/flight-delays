# airport-traffic

### Summary

This project makes day-ahead predictions for flight delays at US airports. Forecasting disruption is useful for airports and airlines as it can be used to deploy mitigating resources to minimise the disruption.

At the time of commit, the model (feed-forward NN) is performing well considering it has only 2 months of flight data - the main data source.

All of the analysis can be found in [analysis](https://github.com/DanielGallant/flight-delays/blob/main/analysis/analysis.ipynb)

### Data sources and Features
Fight data is web scraped daily from 28 US airports using a scheduled Azure Function with Beautiful Soup ([__init__](https://github.com/DanielGallant/flight-delays/blob/main/web_scraper/TimerTrigger1/__init__.py)). Each flight record is categorised as on-time or delayed, then the proportion of flights delayed each day at each airport is calculated (1,2,3 & 5 days looking back).

Minor features include seasonality, public holidays, day of the week, and the airport itself.

### Modelling

The flight delay prediction model (feed-forward NN) is designed to accurately forecast flight delays based on historical data. With an R2 score of 0.6513 and low mean squared error (MSE) and mean absolute error (MAE), the model demonstrates strong predictive capabilities. The model is continuously updated with additional historical data to improve accuracy and enhance its predictive power. Future enhancements may involve incorporating weather forecasts, economic data, and other relevant variables to further enhance the model's performance.
