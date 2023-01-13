
# sqlalchemy in a Flask

This repository contains the code for this [blogpost](https://stackabuse.com/using-sqlalchemy-with-flask-and-postgresql/).


## Getting Started

### Prerequisites

Kindly ensure you have the following installed:
- [ ] [Python 3.6](https://www.python.org/downloads/release/python-365/)
- [ ] [Pip](https://pip.pypa.io/en/stable/installing/)
- [ ] [Virtualenv](https://virtualenv.pypa.io/en/stable/installation/)
- [ ] [PostgreSQL](https://www.postgresql.org/)

### Setting up + Running

1. Clone the repo:

    ```
    $ git clone https://github.com/yagelmai/SQLalchemy_ex.git
    $ cd flask_sqlalchemy_example
    ```

2. With Python 3.6 and Pip installed:

    ```
    $ virtualenv env 
    $ source env/bin/activate
    $ pip install -r requirements.txt
    ```

3. Export the required environment variables:

    ```
    $ export FLASK_APP=app.py
    ```

4. Execute the migrations to create the `sqlalchemydata` table:

    ```
    $ ./env/bin/flask  db migrate
    $ ./env/bin/flask  db upgrade
    ```
   (now the objects from the code is migrate with the DB)

5. Run the Flask API:

    ```
    $ ./env/bin/flask  run
    ```
   (now the app is running first thing is doing the join and return DF and after this its listening to localhost:5000)
6. go to  `http://localhost:5000/run_all`
7. Navigate to `http://localhost:5000/csvjoinmap` to view the the result.
8. you can send post request etc..
