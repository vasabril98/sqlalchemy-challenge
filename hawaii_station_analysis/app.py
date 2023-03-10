import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy as sql
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime

from flask import Flask, jsonify


# Database Setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
measurement = Base.classes.measurement
station = Base.classes.station

# Flask Setup
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"<ul>"
        f"<li>/api/v1.0/precipitation<br/>"
        f"<li>/api/v1.0/stations</li>"
        f"<li>/api/v1.0/tobs</li>"
        f"<li>/api/v1.0/2016-08-23</li>"
        f"<li>/api/v1.0/2016-08-23/2017-07-23</li>"
        f"</ul>"


    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date
    rows = session.scalars(sql.select(measurement.date).order_by(sql.desc(measurement.date))).all()
    recent_date = rows[0]
    
    # Create our date object
    recent_date_object = dt.datetime.strptime(recent_date, '%Y-%m-%d').date()
    
    # Calculate the date one year from the last date in data set.
    year_difference_date = recent_date_object - dt.timedelta(days=365)
    
    # Run query 
    results = session.execute(
        sql.select(measurement.date,measurement.prcp)
        .where(measurement.date > year_difference_date)
        .order_by(measurement.date)
        ).all()

    session.close()
    
    # convert tuple list to dict
    query = dict(results)

    return jsonify(query)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

   
    # Query all passengers
    results = session.execute(
        sql.select(station.name)
        ).all()

    session.close()
    qery_list = list(np.ravel(results))

    return jsonify(qery_list)

@app.route("/api/v1.0/tobs")
def temp_observations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Find the most recent date
    rows = session.scalars(
        sql.select(measurement.date)
        .order_by(sql.desc(measurement.date))
        ).all()
    recent_date = rows[0]
    
    # Create our date object
    recent_date_object = dt.datetime.strptime(recent_date, '%Y-%m-%d').date()

    # Calculate the date one year from the last date in data set.
    year_difference_date = recent_date_object - dt.timedelta(days=365)


    # Find the most active station
    query = session.execute(
        sql.select(measurement.station, func.count(measurement.station) )
        .group_by(measurement.station)
        .order_by(sql.desc(func.count(measurement.station)))).all()
    most_active_id = query[0][0]
   
    # Query the dates and temperature observations of the most-active station for the previous year of data
    results = session.execute(
        sql.select(measurement.date, measurement.tobs)
        .where(sql.and_(measurement.date > year_difference_date, measurement.station == most_active_id))
        .order_by(measurement.date)
        ).all()
    
    # Close the session
    session.close()

    # Convert tuple list to list
    #qery_list = list(np.ravel(results))
    
    # Converting to dictionary instead of list for better representation
    qery_list = dict(results)

    return jsonify(qery_list)


@app.route("/api/v1.0/<start>")
def summary_start(start):

    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Find the earliest and latest date in the table
    first_date = session.scalars(sql.select(measurement.date).order_by(measurement.date)).all()
    last_date = session.scalars(sql.select(measurement.date).order_by(sql.desc(measurement.date))).all()

    # Convert the string dates to datetime objects
    earliest_date = dt.datetime.strptime(first_date[0], '%Y-%m-%d').date()
    lastest_date = dt.datetime.strptime(last_date[0], '%Y-%m-%d').date()
    start_date = dt.datetime.strptime(start, '%Y-%m-%d').date()

    # Check if the start date is in the date range of the tables
    if (earliest_date <= start_date <= lastest_date):
    
    # Quey MIN, AVG, and MAX for all the dates greater than or equal to the start date.
        results = session.execute(
            sql.select(sql.func.min(measurement.tobs),
                    sql.func.max(measurement.tobs),
                    sql.func.avg(measurement.tobs))
            .where(measurement.date >= start_date)
            ).all()
        # Close the session
        session.close()

            # Make a tuple with the labels for the calculations
        names = ("TMIN", "TMAX", "TAVG")
        results = zip(names, results[0])

        # Convert tuple list to list
        #qery_list = list(np.ravel(results))
        
        # Converting to dictionary instead of list for better representation
        qery_list = dict(results)
        return jsonify(qery_list)
    
    # Return if the start date is not in the date range of the tables
    else:
        # Close the session
        session.close()
        return (
            f"The result could not be calculated becasue either:<br/>"
            f"<ul>"
            f"<li>The date provided in the url is not in the date range of the table</li>"
            f"</ul>"
            f"Please choose a date between {earliest_date} and {lastest_date}")
    


@app.route("/api/v1.0/<start>/<end>")
def summary_start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the earliest and latest date in the table
    first_date = session.scalars(sql.select(measurement.date).order_by(measurement.date)).all()
    last_date = session.scalars(sql.select(measurement.date).order_by(sql.desc(measurement.date))).all()

    # Convert the string dates to datetime objects
    earliest_date = dt.datetime.strptime(first_date[0], '%Y-%m-%d').date()
    lastest_date = dt.datetime.strptime(last_date[0], '%Y-%m-%d').date()
    start_date = dt.datetime.strptime(start, '%Y-%m-%d').date()
    end_date = dt.datetime.strptime(end, '%Y-%m-%d').date()


    if ((earliest_date <= start_date <= lastest_date) and 
    (earliest_date <= end_date <= lastest_date) and
    (end_date > start_date)
    ):
        # calculate MIN, AVG, and MAX for all the dates greater than or equal to the start date.
        results = session.execute(
            sql.select(sql.func.min(measurement.tobs),
                    sql.func.max(measurement.tobs),
                    sql.func.avg(measurement.tobs))
            .where(sql.and_(measurement.date >= start_date, measurement.date <= end_date))
            ).all()
        
        session.close()
        
        # Make a tuple with the labels for the calculations
        names = ("TMIN", "TMAX", "TAVG")
        results = zip(names, results[0])

        # Convert tuple list to list
        #qery_list = list(np.ravel(results))
        
        # Converting to dictionary instead of list for better representation
        qery_list = dict(results)
        return jsonify(qery_list)
    
    # Return if the start and end date is not in the date range of the tables
    else:
        session.close()
        return (
                f"The result could not be calculated becasue either:<br/>"
                f"<ul>"
                f"<li>The dates provided in the url is not in the date range of the table</li>"
                f"<li>The start date is greater than the end date</li>"
                f"</ul>"
                f"Please choose dates between {earliest_date} and {lastest_date}"
                )
    
if __name__ == '__main__':
    app.run(debug=True)
