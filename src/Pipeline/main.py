import logging, beautifullogger
import sys
from Pipeline.pipeline import Pipeline
import pandas as pd, numpy as np
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
base_folder= Path(".")/"test"

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
    def location(session, subject):
        return base_folder / subject / session / "song.npy"
    
    @staticmethod
    def compute(out_location: Path, session, subject):
        if out_location.exists():
            return
        res= np.arange(len(session)*len(subject))
        out_location.parent.mkdir(exist_ok=True, parents=True)
        np.save(out_location, res)

@p.register_data()
class SongFilt:
    name="songfilt"

    @staticmethod
    def location(session, subject):
        return base_folder / subject / session / "songfilt.npy"
    
    @staticmethod
    def compute(out_location: Path, session, subject):
        if out_location.exists():
            return
        songloc = p.get_single_location("song", session=session, subject=subject)
        if not songloc.exists():
            p.compute("song", session=session, subject=subject)
        s = np.load(songloc)
        res = np.cumsum(s)
        out_location.parent.mkdir(exist_ok=True, parents=True)
        np.save(out_location, res)

def run():
    setup_nice_logging()
    logger.info("Running start")
    print(p.get_coords("session", subject=slice("s1", "s3")))
    p.compute("songfilt")
    logger.info("Running end")