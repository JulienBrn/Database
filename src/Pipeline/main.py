import logging, beautifullogger
import sys
from Pipeline.pipeline import Pipeline, cache
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

@p.register_coord(["subject", "stupid"])
def subject():
    return pd.DataFrame([["s1", "toto"], ["s2", "tata"], ["s1", "titi"]], columns=["subject", "stupid"])

@p.register_coord(["session"])
def session(subject):
    if subject == "s1":
        return pd.DataFrame([["se1" + subject], ["se2"]], columns=["session"])
    else:
        return pd.DataFrame([["se3"]], columns=["session"])

@p.register_coord(["fs"])
def fs():
    return [10, 20]

@p.register_coord(["test"])
def test(subject):
    return pd.DataFrame([[5+int(subject[1])], [10]], columns=["test"])

@p.register_data()
class Song:

    name = "song"
    
    @staticmethod
    def location(session, subject):
        return base_folder / subject / session / "song.npy"
    
    @staticmethod
    @cache(saver=np.save, open="wb")
    def compute(out_location: Path, selection):
        res= np.arange(len(selection["session"])*len(selection["subject"]))
        return res

@p.register_data()
class SongFilt:
    name="songfilt"

    @staticmethod
    def location(session, subject):
        return base_folder / subject / session / "songfilt.npy"
    
    @staticmethod
    @cache(saver=np.save, open="wb")
    def compute(out_location: Path, selection):
        songloc = p.get_single_location("song", selection)
        if not songloc.exists():
            p.compute("song", selection)
        s = np.load(songloc)
        res = np.cumsum(s)
        return res

def run():
    global p
    setup_nice_logging()
    logger.info("Running start")
    p = p.initialize()
    print(p.get_coords(["subject"]
                    #    , subject=slice("s1", "s3")
                    #    , session="se2"
                    ))
    print(p.compute("songfilt"))
    logger.info("Running end")