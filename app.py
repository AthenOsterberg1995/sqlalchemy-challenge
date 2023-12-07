# Import the dependencies.
from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import pandas as pd
import datetime as dt
#for date calculations
#https://stackoverflow.com/questions/5871168/how-can-i-subtract-or-add-100-years-to-a-datetime-field-in-the-database-in-djang
from dateutil.relativedelta import relativedelta

#################################################
# Database Setup
#################################################
#all copied from climate ipynb file
db_path = "/Users/athenosterberg/Desktop/Classwork/sqlalchemy-challenge/Resources/hawaii.sqlite"
engine = create_engine(f"sqlite:///{db_path}")
#engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB


#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    return (
        f"Welcome to the Precipitation API<br/>"
        f"Available Routes:<br/>"
        f"/precipitation<br/>"
        f"/station<br/>"
        f"/tobs<br/>"
        f"/start<br/>"
        f"/start/end<br/>"
        f"date format for start and start/end is yyyy-mm-dd"
    )

@app.route("/precipitation")
def precipitation():
    session = Session(bind=engine)
    #code from climate ipynb
    recent_date = session.query(Measurement.date).order_by((Measurement.date).desc()).limit(1).all()
    date_str_list = recent_date[0][0].split("-")
    #subtract year
    #https://stackoverflow.com/questions/5871168/how-can-i-subtract-or-add-100-years-to-a-datetime-field-in-the-database-in-djang
    date_val = dt.date(int(date_str_list[0]),int(date_str_list[1]),int(date_str_list[2]))
    start_time = date_val - relativedelta(years=1)
    # Perform a query to retrieve the data and precipitation scores
    last_year_query = session.query(Measurement.prcp, Measurement.date).filter(Measurement.date > start_time).\
                      order_by(Measurement.date).all()
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    year_precipitation_df = pd.DataFrame(last_year_query,columns=["precipitation","date"])
    year_precipitation_df["date"] = year_precipitation_df["date"].astype(str)
    # Sort the dataframe by date
    #sorting code
    #https://www.geeksforgeeks.org/how-to-sort-pandas-dataframe/
    year_precipitation_df.sort_values(by=["date"])
    #year_precipitation_json = year_precipitation_df.to_json()
    year_precipitation_json = []
    #df looping
    #https://www.geeksforgeeks.org/different-ways-to-iterate-over-rows-in-pandas-dataframe/
    #creating the json this way so output is readable
    for index in year_precipitation_df.index:
        year_precipitation_json.append({"Date":year_precipitation_df["date"][index],"Precipitation":year_precipitation_df["precipitation"][index]})
    session.close()
    return year_precipitation_json

@app.route("/station")
def stations():
    session = Session(bind=engine)
    station_query = session.query(Station.station).all()
    #code for converting from row format
    #https://stackoverflow.com/questions/71724579/row-is-not-json-serializable-error-when-sending-result-set-to-a-flask-view
    station_tuple = [tuple(row) for row in station_query]
    station_json = []
    for station in station_tuple:
        station_json.append({"Station":station})
    session.close()
    return jsonify(station_json)

@app.route("/tobs")
def tobs():
    session = Session(bind=engine)
    #copied from climate ipynb
    active_station_query = session.query(Measurement.station, func.count(Measurement.station)).\
                       group_by(Measurement.station).\
                       order_by(func.count(Measurement.station).desc()).all()
    recent_date_station = session.query(Measurement.date).\
                      filter(Measurement.station == active_station_query[0][0]).\
                      order_by((Measurement.date).desc()).limit(1).all()
    date_str_list = recent_date_station[0][0].split("-")
    date_val = dt.date(int(date_str_list[0]),int(date_str_list[1]),int(date_str_list[2]))
    start_time = date_val - relativedelta(years=1)
    year_temps = session.query(Measurement.tobs).filter(Measurement.station == active_station_query[0][0]).filter(Measurement.date > start_time).all()
    temps_df = pd.DataFrame(year_temps,columns=["tobs"])
    temps_json = temps_df.to_json()
    session.close()
    return temps_json

@app.route("/<start>")
def start(start):
    session = Session(bind=engine)
    #query with filter for variable
    date_query = session.query(func.avg(Measurement.tobs), func.min(Measurement.tobs), func.max(Measurement.tobs)).\
                 filter(Measurement.date > start).all()
    #convert row format to tuple for json conversion
    date_tuple = [tuple(row) for row in date_query]
    print(date_tuple[0][0])
    date_json = {"Average Temperature":date_tuple[0][0],"Minimum Temperature":date_tuple[0][1],"Maximum Temperature":date_tuple[0][2]}
    if date_json["Average Temperature"] == None and date_json["Minimum Temperature"] == None and date_json["Maximum Temperature"] == None:
        return (jsonify("Invalid date")), 404
    else:
        return date_json

@app.route("/<start>/<end>")
def startend(start, end):
    session = Session(bind=engine)
    date_query = session.query(func.avg(Measurement.tobs), func.min(Measurement.tobs), func.max(Measurement.tobs)).\
                 filter(Measurement.date > start).filter(Measurement.date <= end).all()
    date_tuple = [tuple(row) for row in date_query]
    print(date_tuple[0][0])
    date_json = {"Average Temperature":date_tuple[0][0],"Minimum Temperature":date_tuple[0][1],"Maximum Temperature":date_tuple[0][2]}
    if date_json["Average Temperature"] == None and date_json["Minimum Temperature"] == None and date_json["Maximum Temperature"] == None:
        return (jsonify("Invalid date range")), 404
    else:
        return date_json

if __name__ == '__main__':
    app.run(debug=True)

