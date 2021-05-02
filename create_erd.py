import configparser
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData

def main():
    """
    - Gets the configuration properties from the dwh.cfg file
    - Creates the ER diagram
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    graph = create_schema_graph(metadata=MetaData("postgresql://{}:{}@{}:{}/{}".format(config['CLUSTER']['DB_USER'], 
                                                                                       config['CLUSTER']['DB_PASSWORD'], 
                                                                                       config['CLUSTER']['HOST'], 
                                                                                       config['CLUSTER']['DB_PORT'], 
                                                                                       config['CLUSTER']['DB_NAME'])))
    
    graph.write_png('sparkifydb_erd.png')

if __name__ == "__main__":
    main()