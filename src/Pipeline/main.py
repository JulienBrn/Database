import logging, beautifullogger
import sys
from Pipeline.pipeline import Pipeline
import pandas as pd
from pathlib import Path
logger = logging.getLogger(__name__)


def setup_nice_logging():
    beautifullogger.setup(logmode="w")
    logging.getLogger("toolbox.ressource_manager").setLevel(logging.WARNING)
    logging.getLogger("toolbox.signal_analysis_toolbox").setLevel(logging.WARNING)

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        logger.info("Keyboard interupt")
        sys.exit()
        return
    else:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception


p = Pipeline()
base_folder= Path(".")

@p.register_coord()
def subject():
    return pd.DataFrame([["s1", "toto"], ["s2", "titi"]], columns=["subject", "stupid"])

@p.register_coord()
def session(subject, stupid):
    if subject == "s1":
        return pd.DataFrame([["se1" + stupid], ["se2"]], columns=["session"])
    elif subject == "s2":
        return pd.DataFrame([["se3"]], columns=["session"])

@p.register_data()
class Song:

    name = "song"
    
    @staticmethod
    def location(session):
        return list((base_folder / session / "song").glob("*.npy"))[0]
    
    @staticmethod
    def compute(out_location, session):
        import json
        metadata = p.get_single_location("carmen_metadata", session=session)
        if not metadata.exists():
            p.compute("carmen_metadata", session=session)
        metadata = json.load(metadata.open("r"))
        return metadata["test"]*2




def run():
    setup_nice_logging()
    logger.info("Running start")
    print(p.get_coords("session", subject=slice("s1", "s3")))
    logger.info("Running end")