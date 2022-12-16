from typing import List, Tuple, Any
from psycopg2 import sql

import Utility.DBConnector as Connector
from Business.Actor import Actor
from Business.Critic import Critic
from Business.Movie import Movie
from Business.Studio import Studio
from Utility.Exceptions import DatabaseException
from Utility.ReturnValue import ReturnValue


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Critics("
                     "id INTEGER PRIMARY KEY,"
                     "name TEXT NOT NULL)"
                     )
        conn.execute("CREATE TABLE Movies("
                     "name TEXT,"
                     "year INTEGER CHECK(year >= 1895),"
                     "genere TEXT CHECK(genere IN ('Drama', 'Action', 'Comedy', 'Horror')) NOT NULL,"
                     "PRIMARY KEY (name, year))"
                     )
        conn.execute("CREATE TABLE Actors("
                     "id INTEGER PRIMARY KEY CHECK(id > 0),"
                     "name TEXT NOT NULL,"
                     "age INTEGER NOT NULL CHECK(age > 0),"
                     "height INTEGER NOT NULL CHECK(height > 0))"
                     )
        conn.execute("CREATE TABLE Studios("
                     "id INTEGER PRIMARY KEY,"
                     "name TEXT NOT NULL)"
                     )
        conn.execute(
            "CREATE TABLE CriticsMovie("
            "critic_id INTEGER ,"
            "movie_name TEXT NOT NULL,"
            "movie_year INTEGER,"
            "rating INTEGER NOT NULL,"
            "FOREIGN KEY (critic_id) REFERENCES Critics(id),"
            "FOREIGN KEY (movie_name, movie_year) REFERENCES Movies(name, year))"
        )
        conn.execute(
            "CREATE TABLE StudiosMovie("
            "studio_id INTEGER,"
            "movie_name TEXT NOT NULL,"
            "movie_year INTEGER,"
            "budget INTEGER NOT NULL,"
            "revenue INTEGER NOT NULL,"
            "FOREIGN KEY (studio_id) REFERENCES Studio(id),"
            "FOREIGN KEY (movie_name, movie_year) REFERENCES Movies(name, year))"
        )
        conn.execute(
            "CREATE TABLE ActorsMovie("
            "actor_id INTEGER,"
            "movie_name TEXT NOT NULL,"
            "movie_year INTEGER,"
            "salary INTEGER NOT NULL,"
            "roles LIST(TEXT NOT NULL)"
            "FOREIGN KEY (actor_id) REFERENCES Actors(id),"
            "FOREIGN KEY (movie_name, movie_year) REFERENCES Movies(name, year))"
        )
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def clearTables():
    # TODO: implement
    pass


def dropCritics():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Critics CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropActors():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Actors CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropMovies():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Movies CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropStudios():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Studios CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropCriticsMovie():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS CriticsMovie CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropActorsMovie():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS ActorsMovie CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropStudiosMovie():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS StudiosMovie CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropTables():
    dropCritics()
    dropMovies()
    dropActors()
    dropStudios()
    dropStudiosMovie()
    dropActorsMovie()
    dropCriticsMovie()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO Critics(id, name) VALUES({id}, {username})").format(
            id=sql.Literal(critic.getCriticID()), username=sql.Literal(critic.getName())))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def addActor(actor: Actor) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO Actors(id, name, age, height) VALUES("
                    "{id}, {name}, {age}, {height})").format(
            id=sql.Literal(actor.getActorID()),
            name=sql.Literal(actor.getActorName()),
            age=sql.Literal(actor.getAge()),
            height=sql.Literal(actor.getHeight())))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO Studios(id, name) VALUES({id}, {name})").format(
            id=sql.Literal(studio.getStudioID()),
            name=sql.Literal(studio.getStudioName())))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(sql.SQL(f"DELETE FROM Critics WHERE id={sql.Literal(critic_id)}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    try:
        conn = Connector.DBConnector()
        _, (name) = conn.execute(sql.SQL(f"SELECT name FROM Critics WHERE id={critic_id}"))
        return Critic(critic_id, name)
    except Exception as e:
        catchException(e, conn)
        return None


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(sql.SQL(f"DELETE FROM Actors WHERE id={sql.Literal(actor_id)}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    try:
        conn = Connector.DBConnector()
        _, (name, age, height) = conn.execute(sql.SQL(f"SELECT name, age, height FROM Critics WHERE id={actor_id}"))
        return Actor(actor_id, name, age, height)
    except Exception as e:
        catchException(e, conn)
        return None


def addMovie(movie: Movie) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO Movies(name, year, genere) VALUES({name}, {year}, {genere})").format(
            name=sql.Literal(movie.getMovieName()), year=sql.Literal(movie.getYear()),
            genere=sql.Literal(movie.getGenre())))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def catchException(e: Exception, conn: Any) -> ReturnValue:
    try:
        raise e
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return ReturnValue.OK


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL(f"DELETE FROM Movies WHERE name={sql.Literal(movie_name)} AND year={sql.Literal(year)}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    try:
        conn = Connector.DBConnector()
        _, (genere) = conn.execute(sql.SQL(f"SELECT genere FROM Critics WHERE name={movie_name} AND year={year}"))
        return Movie(movie_name, year, genere)
    except Exception as e:
        catchException(e, conn)
        return None


def deleteStudio(studio_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL(f"DELETE FROM Movies WHERE if={sql.Literal(studio_id)}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    try:
        conn = Connector.DBConnector()
        _, (name) = conn.execute(sql.SQL(f"SELECT name FROM Studios WHERE id={studio_id}"))
        print(_,studio_id,  name)
        return Studio(studio_id, name)
    except Exception as e:
        catchException(e, conn)
        return None


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO CriticsMovie(critic_id, movie_name, movie_year, rating) VALUES("
            "{id}, {name}, {year}, {rating})").format(
            id=sql.Literal(criticID), name=sql.Literal(movieName),
            year=sql.Literal(movieYear), rating=sql.Literal(rating)))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL(f"DELETE FROM CriticsMovie WHERE movie_name={sql.Literal(movieName)} AND "
            f"movie_year={sql.Literal(movieYear)} AND critic_id={criticID}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO CriticsMovie(actor_id, movie_name, movie_year, salary, roles) VALUES("
            "{id}, {name}, {year}, {salary}, {roles})").format(
            id=sql.Literal(actorID), name=sql.Literal(movieName), year=sql.Literal(movieYear),
            rating=sql.Literal(salary), roles=sql.Literal(roles)))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL(f"DELETE FROM ActoresMovie WHERE movie_name={sql.Literal(movieName)} AND "
            f"movie_year={sql.Literal(movieYear)} AND actor_id={actorID}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("INSERT INTO CriticsMovie(studio_id, movie_name, movie_year, budget, revenue) VALUES("
            "{id}, {name}, {year}, {budget}, {revenue})").format(
            id=sql.Literal(studioID), name=sql.Literal(movieName), year=sql.Literal(movieYear),
            budget=sql.Literal(budget), revenue=sql.Literal(revenue)))
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL(f"DELETE FROM StudiosMovie WHERE movie_name={sql.Literal(movieName)} AND "
            f"movie_year={sql.Literal(movieYear)} AND studio_id={studioID}"))
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def studioRevenueByYear() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def getMovies(printSchema: bool = False):
    conn = None
    Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("SELECT * FROM Movies", printSchema=printSchema)
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getActors(printSchema: bool = False):
    conn = None
    Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("SELECT * FROM Actors", printSchema=printSchema)
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getStudios(printSchema: bool = False):
    conn = None
    Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("SELECT * FROM Studios", printSchema=printSchema)
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


def getCritics(printSchema: bool = False):
    conn = None
    Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("SELECT * FROM Critics", printSchema=printSchema)
    except Exception as e:
        return catchException(e, conn)
    if conn is not None:
        conn.close()
    return ReturnValue.OK


# GOOD LUCK!
if __name__ == '__main__':
    dropTables()
    createTables()
    addCritic(Critic(1, "Moshe"))
    addCritic(Critic(2, "Yagel"))
    addCritic(Critic(3, "Avigail"))
    addMovie(Movie("Best Movie", 2000, 'Action'))
    addMovie(Movie("Worst Movie", 1990, 'Horror'))
    addMovie(Movie("Ok Movie", 2005, 'Comedy'))
    addActor(Actor(1, "Hilbert", 4, 8))
    addActor(Actor(8, "So-Yang", 16, 5))
    addActor(Actor(45, "Luna", 70, 6))
    addActor(Actor(13, "Miley", 13, 9))
    addActor(Actor(2, "Gon", 34, 1))
    addStudio(Studio(5, "Baloo"))
    addStudio(Studio(6, "Shick"))
    print('critics:')
    getCritics(printSchema=True)
    print('movies:')
    getMovies(printSchema=True)
    print('actors:')
    getActors(printSchema=True)
    print('studios:')
    getStudios(printSchema=True)
    criticRatedMovie("Best Movie", 2000, 1, 5)
    Critic = getCriticProfile(1)
    # print("we found the critic id1 is: " + str(Critic.getName()))
