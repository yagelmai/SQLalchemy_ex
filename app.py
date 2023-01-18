from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemycollector import setup, MetisInstrumentor, PlanCollectType
import time
from numpy import genfromtxt
import csv
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.sql.expression import text
from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd


app = Flask(__name__)
with app.app_context():
    app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://yagel:postgres@localhost:5432/sqlalchemydata"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db = SQLAlchemy(app)
    migrate = Migrate(app, db)

    # instrumentation: MetisInstrumentor = setup('power_csvs-web-server',
    #                                            api_key='<API_KEY>',
    #                                            service_version='1.1'
    #                                            )
    #
    # instrumentation.instrument_app(app, db.get_engine())


    class PowerCsv(db.Model):
        __tablename__ = 'par_exe_csv'

        id = db.Column(db.Integer, primary_key=True)
        cell_name = db.Column(db.String())
        test = db.Column(db.String())
        flopcnt = db.Column(db.Integer())
        # map_file = db.relationship('PowerMapfile', primaryjoin='PowerMapfile.csv_id==PowerCsv.id', backref='par_exe_csv')

        def __init__(self, cell_name, test, flopcnt):
            self.cell_name = cell_name
            self.test = test
            self.flopcnt = flopcnt

        def __repr__(self):
            return f"<par_exe_csv-{self.cell_name}>"

    class PowerMapfile(db.Model):
        __tablename__ = 'par_exe_mapfile'

        id = db.Column(db.Integer, primary_key=True)
        dlvrloadgndvsense0 = db.Column(db.String())
        dlvrloadgndvsense02 = db.Column(db.String())
        # csv_id = db.Column(db.Integer, db.ForeignKey('par_exe_csv.id'))

        def __init__(self, dlvrloadgndvsense0, dlvrloadgndvsense02):
            self.dlvrloadgndvsense0 = dlvrloadgndvsense0
            self.dlvrloadgndvsense02 = dlvrloadgndvsense02

        def __repr__(self):
            return f"<par_exe_mapfile-{self.cell_name}>"

    class csvjoinmap(db.Model):
        __tablename__ = 'csvjoinmap'

        id = db.Column(db.Integer, primary_key=True)
        cell_name = db.Column(db.String())
        dlvrloadgndvsense0 = db.Column(db.String())
        # csv_id = db.Column(db.Integer, db.ForeignKey('par_exe_csv.id'))

        def __init__(self, cell_name, dlvrloadgndvsense0):
            self.cell_name = cell_name
            self.dlvrloadgndvsense0 = dlvrloadgndvsense0

        def __repr__(self):
            return f"<csvjoinmap-{self.id}>"

    @app.route('/')
    def hello():
        return {"hello": "world"}


    def Load_Data(file_name):
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda s: str(s)})
        return data.tolist()
    @app.route('/load_csv')
    def loadCsv():
        t=time.time()
        try:
            file_name = '../../../out/par_exe.power.csv'
            data = Load_Data(file_name)
            for i in data:
                record = PowerCsv(**{
                    # 'date': datetime.strptime(i[0], '%d-%b-%y').date(),
                    'cell_name': i[0],
                    'test': i[1],
                    'flopcnt': i[4],
                })
                db.session.add(record)  # Add all the records

            db.session.commit()  # Attempt to commit all the records
            return {"res": "sucess"}
        except Exception as e:
            print(e)
            db.session.rollback()  # Rollback the changes on error
            return {"res": "failed"}
        finally:
            db.session.close()  # Close the connection
            print("Time elapsed: " + str(time.time() - t) + " s.")

    @app.route('/load_mapfile')
    def loadMapfile():
        t = time.time()
        try:
            file_name = '../../../out/par_exe.rtl.mapfile'
            data = Load_Data(file_name)
            for i in data:
                record = PowerMapfile(**{
                    # 'date': datetime.strptime(i[0], '%d-%b-%y').date(),
                    'dlvrloadgndvsense0': i[0],
                    'dlvrloadgndvsense02': i[1]
                })
                db.session.add(record)  # Add all the records

            db.session.commit()  # Attempt to commit all the records
            return {"res": "sucess"}
        except Exception as e:
            print(e)
            db.session.rollback()  # Rollback the changes on error
            return {"res": "failed"}
        finally:
            db.session.close()  # Close the connection
            print("Time elapsed: " + str(time.time() - t) + " s.")

    @app.route('/join')
    def joinTables():
        t = time.time()
        try:
            # Join the two tables
            result = db.session.query(
                PowerCsv.cell_name, PowerMapfile.dlvrloadgndvsense0).filter(
                func.substring(PowerCsv.cell_name, 3, 2) == func.substring(
                    PowerMapfile.dlvrloadgndvsense0, 3, 2)
            ).limit(
                100).all()

            # Insert the results into the "res" table
            for res in result:
                db.session.add(csvjoinmap(cell_name=res[0], dlvrloadgndvsense0=res[1]))
            db.session.commit()
            return {"res": "sucess"}
        except Exception as e:
            print(e)
            db.session.rollback()  # Rollback the changes on error
            return {"res": "failed"}
        finally:
            db.session.close()
            print("Time elapsed: " + str(time.time() - t) + " s.")


    @app.route('/createDF')
    def CreateDf():
        try:
            df = pd.read_sql_table('csvjoinmap', db.engine)
            return df.to_json()
        except Exception as e:
            print(e)
            return {"res": "failed", "reas": str(e)}


    @app.route('/run_all')
    def runAll():
        f_time = time.time()
        loadCsv()
        loadMapfile()
        s_time = time.time()
        joinTables()
        t_time = time.time()
        df = CreateDf()
        l_time = time.time()
        print("successful finished! at time: ")
        print(l_time - f_time)
        print("copy csv and mapfile to tables: ")
        print(s_time - f_time)
        print("join: ")
        print(t_time - s_time)
        print("create DF: ")
        print(l_time - t_time)
        return df


    @app.route('/par_exe_csv', methods=['POST', 'GET'])
    def handlePowerCsvs():
        if request.method == 'POST':
            if request.is_json:
                data = request.get_json()
                new_power_csv = PowerCsv(cell_name=data['cell_name'], test=data['test'], flopcnt=data['flopcnt'])

                db.session.add(new_power_csv)
                db.session.commit()

                return {"message": f"par_exe_csv {new_power_csv.cell_name} has been created successfully."}
            else:
                return {"error": "The request payload is not in JSON format"}

        elif request.method == 'GET':
            power_csvs = PowerCsv.query.all()
            results = [
                {
                    "cell_name": power_csv.cell_name,
                    "test": power_csv.test,
                    "flopcnt": power_csv.flopcnt
                } for power_csv in power_csvs]

            return {"count": len(results), "power_csvs": results, "message": "success"}


    @app.route('/power_csvs/<power_id>', methods=['GET', 'PUT', 'DELETE'])
    def handlePowerCsv(power_id):
        power_csv = PowerCsv.query.get_or_404(power_id)

        if request.method == 'GET':
            response = {
                "cell_name": power_csv.cell_name,
                "test": power_csv.test,
                "flopcnt": power_csv.flopcnt
            }
            return {"message": "success", "power_csv": response}

        elif request.method == 'PUT':
            data = request.get_json()
            power_csv.cell_name = data['cell_name']
            power_csv.test = data['test']
            power_csv.flopcnt = data['flopcnt']

            db.session.add(power_csv)
            db.session.commit()

            return {"message": f"power_csv {power_csv.cell_name} successfully updated"}

        elif request.method == 'DELETE':
            db.session.delete(power_csv)
            db.session.commit()

            return {"message": f"power_csv {power_csv.cell_name} successfully deleted."}

    if __name__ == '__main__':
        app.run(debug=True)