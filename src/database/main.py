import logging, beautifullogger
import sys
from database.database import DatabaseInstance, Database, cache, Data, CoordComputer, singleglob
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


p = Database("test")
# carmen_folder= Path("/media/filer2/T4b/Carmen/LMANX_correlations_project/LMANX_behavior_data/BirdData/")
# base_folder = Path("./test")

# @p.register
# @Data.from_class
# class AllSongFolders:
#     name = "all_song_folders"

#     @staticmethod
#     def location():
#         return  base_folder/"all_song_folders.txt"
    
#     @staticmethod
#     @cache(lambda *args: np.savetxt(*args, fmt="%s"))
#     def compute(out_location: Path, selection):
#         return np.array([str(f.relative_to(carmen_folder)) for f in carmen_folder.glob("**/song")])
    
# @p.register
# @CoordComputer.from_function(vectorized=False, adapt_return=True)
# def session():
#     p.compute("all_song_folders")
#     l=np.loadtxt(p.get_single_location("all_song_folders"), dtype=str)
#     all = [str(Path(f).parent) for f in l if not "UnusedRecordings" in f]
#     return all

# @p.register
# @CoordComputer.from_function(vectorized=False, adapt_return=True)
# def subject(session: str):
#     ret = str(Path(session).parents[-2])
#     return [ret]

# @p.register
# @CoordComputer.from_function(coords=["date"], dependencies=["session"], vectorized=True, adapt_return=False)
# def date(df: pd.DataFrame):
#     def simple_extract(session):
#         return session.str.extract("(\d{4}-\d{2}-\d{2})", expand=False)
#     def simple_extract2(session):
#         return session.str.extract("(\d{4}_\d{2}_\d{2})", expand=False).str.replace("_", "-")
#     def strange_extract(session):
#         tmp = session.str.extract("(\d{4}_\d{4})", expand=False)
#         return tmp.str.slice_replace(7, 7, repl="_").str.replace("_", "-")
#     def reversed_extract(session):
#         tmp = session.str.extract("(\d{2}_\d{2}_\d{4})", expand=False)
#         return tmp.str.slice(6, None) + "-"+ tmp.str.slice(3, 5) + "-"+ tmp.str.slice(0, 2)
#     def failed_extract(session):
#         return "Unknown"
    
#     df["date"] = np.nan
#     for f in [simple_extract, simple_extract2, strange_extract, reversed_extract, failed_extract]:
#         df["date"] = np.where(pd.isna(df["date"]), f(df["session"]), df["date"])

#     return df


# @p.register
# @Data.from_class
# class RawNeuroData:
#     name = "raw_session_data"

#     @staticmethod
#     def location(session):
#         try:
#             return singleglob(carmen_folder / session, "*.smr", "*.smrx", "*.SMR", "*.SMRX")
#         except FileNotFoundError:
#             return singleglob(carmen_folder / session, "**/bua/raw_traces")

    

@p.register
@CoordComputer.from_function(vectorized=False, adapt_return=True)
def recording_source(session: str):
    l: Path = p.get_single_location("raw_session_data", session=session)
    if "smr" in l.suffix.lower():
        return ["spike2"]
    elif not l.suffix:
        return ["neuropixel"]
    else:
        return ["Unknown"]
    

    
# @p.register
# @Data.from_class
# class RawNeuroMetadata:
#     name = "raw_session_metadata"

#     @staticmethod
#     def location(session):
#         return carmen_folder / session / 



# @p.register
# @CoordComputer.from_function(vectorized=False, adapt_return=True)
# def raw_electrophy_signal(session: str):
#     ret = str(Path(session).parents[-2])
#     return [ret]

# @p.register
# @CoordComputer.from_function(vectorized=False, adapt_return=True)
# def spike_signals(session: str, raw_electrophy_signal):
#     ret = str(Path(session).parents[-2])
#     return [ret]


# @p.register_coord(["session"])
# def session(subject):
#     if subject == "s1":
#         return pd.DataFrame([["se1" + subject], ["se2"]], columns=["session"])
#     else:
#         return pd.DataFrame([["se3"]], columns=["session"])

# @p.register_coord(["fs"])
# def fs():
#     return [10, 20]

# @p.register_coord(["test"])
# def test(subject):
#     return pd.DataFrame([[5+int(subject[1])], [10]], columns=["test"])

# @p.register_data()
# class Song:

#     name = "song"
    
#     @staticmethod
#     def location(session, subject):
#         return base_folder / subject / session / "song.npy"
    
#     @staticmethod
#     @cache(saver=np.save, open="wb")
#     def compute(out_location: Path, selection):
#         res= np.arange(len(selection["session"])*len(selection["subject"]))
#         return res

# @p.register_data()
# class SongFilt:
#     name="songfilt"

#     @staticmethod
#     def location(session, subject):
#         return base_folder / subject / session / "songfilt.npy"
    
#     @staticmethod
#     @cache(saver=np.save, open="wb")
#     def compute(out_location: Path, selection):
#         songloc = p.get_single_location("song", selection)
#         if not songloc.exists():
#             p.compute("song", selection)
#         s = np.load(songloc)
#         res = np.cumsum(s)
#         return res

def run():
    global p
    setup_nice_logging()
    logger.info("Running start")
    p = p.initialize()
    print(p.get_coords(["session", "subject", "date", "neuro_recording_source"]))
    # p.compute("songfilt", subject="s2")
    logger.info("Running end")