from typing import Dict, Tuple, NamedTuple, List, Callable, Any, Protocol, Container, Mapping
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
import inspect, functools
import collections, collections.abc

class ComputeProtocol(Protocol):
    def meth(self, out_location, coords: Dict[str, Any]) -> None: ...





@dataclass
class CoordComputer:
    coords: set[str]
    dependencies: set[str]
    compute: Callable[[Mapping[str, Any]], pd.DataFrame]

    @staticmethod
    def from_function(coords=None, dependencies=None, vectorized=False, adapt_return=True):
        def decorate(f):
            nonlocal coords, dependencies, vectorized, adapt_return
            if coords is None:
                coords=set([f.__name__])
            if dependencies is None:
                if vectorized:
                    raise Exception("Unable to determine dependencies")
                else:
                    dependencies = set(inspect.signature(f).parameters.keys())
            if adapt_return:
                @functools.wraps(f)
                def new_f(*args, **kwargs):
                    res = f(*args, **kwargs)
                    if not isinstance(res, pd.DataFrame):
                        res = pd.DataFrame(res, columns=list(coords))
                    if not set(coords).issubset(set(res.columns)):
                        raise Exception(f"Problem in coord definition")
                    return res
            else:
                new_f = f
            if not vectorized:
                def compute(df: pd.DataFrame):
                    tmp_dfs = []
                    for _, row in df.iterrows():
                        d = row.to_dict()
                        try:
                            tmp = new_f(**d)
                        except Exception as e:
                            e.add_note(f'Parameters are {d}')
                            raise e
                        for k,v in d.items():
                            tmp[k] = v
                        tmp_dfs.append(tmp)
                    return pd.concat(tmp_dfs)
            else:
                compute= new_f
            f._pipelineobject = CoordComputer(coords=set(coords), dependencies=set(dependencies), compute=compute)
            return f
        return decorate
            
@dataclass
class Data:
    name: str
    coords: set[str]
    get_location: Callable[[Mapping[str, Any]], Any]
    compute: ComputeProtocol | None

    @staticmethod
    def from_class(cls):
        if not hasattr(cls, "location"):
            raise Exception("No location function")
        loc_func = getattr(cls, "location")
        loc_func_args = set(inspect.signature(loc_func).parameters.keys())
        compute_func = None if not hasattr(cls, "compute") else getattr(cls, "compute")
        name = getattr(cls, "name") if hasattr(cls, "name") else cls.__name__ 
        cls._pipelineobject = Data(name=name,coords=loc_func_args, get_location=lambda d: loc_func(**{k: v for k,v in d.items() if k in loc_func_args}), compute=compute_func)
        return cls

class Pipeline:
    coord_computers = List[CoordComputer]
    data: Dict[str, Data]

    def __init__(self):
        self.coord_computers = []
        self.data = {}

    def declare(self, o):
        # print(f"declaring {o}")
        if isinstance(o, CoordComputer):
            self.coord_computers.append(o)
        elif isinstance(o, Data):
            if o.name in self.data:
                raise Exception(f"Data {o.name} already exists")
            self.data[o.name] = o
        else:
            raise Exception(f'Cannot declare object of type {type(o)}')

    def register(self, f):
        self.declare(f._pipelineobject)
        return f

    def initialize(self):
        # print(list(self.data.keys()))
        if hasattr(self, "instance"):
            raise Exception("Cannot be initialized twice")
        inst = PipelineInstance(self)
        self.instance = inst
        return inst
    
    def __getattr__(self, name: str) -> Any:
        if name != "instance":
            if hasattr(self, "instance"):
                try:
                    return self.instance.__getattribute__(name)
                except Exception as e:
                    e.add_note(f'getaatr, name={name}')
                    raise
            else:
                raise AttributeError()
        else:
            raise AttributeError()
        

class PipelineInstance:
    p: Pipeline
    coords: Dict[str, CoordComputer]

    def __init__(self, p: Pipeline):
        self.p = p
        self.coords = {}
        for cc in p.coord_computers:
            for c in cc.coords:
                if c in self.coords:
                    raise Exception(f"Coordinate {c} has two computers")
                else:
                    self.coords[c] = cc
    

    def _get_dependencies(self, names: set[str]):
        res = set()
        for n in names:
            res = res.union(set([n]), self._get_dependencies(self.coords[n].dependencies))
        return res
    
    def _filter(self, df: pd.DataFrame, selection_dict):
        for s, v in selection_dict.items():
            if s in df.columns:
                if isinstance(v, tuple):
                    df = df.loc[df[s].isin(v)]
                elif isinstance(v, slice):
                    if v.step:
                        Exception("step not handled")
                    start_cond = df[s] >= v.start if v.start else True
                    end_cond = df[s] < v.stop if v.stop else True
                    df = df.loc[start_cond & end_cond]
                else:
                    df = df.loc[df[s] ==v]
        return df
    
    def get_coords(self, names, selection_dict = {}, **selection_kwargs):
        from frozendict import frozendict
        selection_dict = selection_dict | selection_kwargs
        if isinstance(names, str):
            names = [names]
        return self.cached_get_coords(frozenset(names), frozendict(selection_dict))
        
    @functools.cache
    def cached_get_coords(self, names: set[str], selection_dict):
        
        all_names = self._get_dependencies(names)
        
        df = pd.DataFrame([[]])
        while not all_names.issubset(set(df.columns)):
            for cc in self.p.coord_computers:
                if cc.coords.intersection(all_names) != set() and cc.dependencies.issubset(set(df.columns)) and not cc.coords.issubset(set(df.columns)):
                    # tmp_dfs = []
                    dep_list = list(cc.dependencies)
                    param_df = df[dep_list].drop_duplicates()
                    try:
                        tmp_df = self._filter(cc.compute(param_df), selection_dict)
                    except Exception as e:
                        e.add_note(f'While computing {cc.coords}')
                        raise e
                    df = pd.merge(df, tmp_df, on=list(cc.dependencies), how="inner") if len(dep_list) > 0 else pd.merge(df, tmp_df, how="cross") 
                    
        def keep_unique_columns(df):
            cols = []
            for col in df.columns:
                if (df[col] == df[col].iat[0]).all():
                    cols.append(col)
            return df[cols].head(1)
        if len(names) > 0:
            res = df.groupby(list(names), group_keys=False).apply(keep_unique_columns)
        else:
            return df
        return res
    
    def get_locations(self, name, selection_dict={}, **selection_kwargs) -> pd.DataFrame:
        coords = self.get_coords(self.p.data[name].coords, selection_dict, **selection_kwargs)
        coords["location"] = coords.apply(lambda row: self.p.data[name].get_location(row.to_dict()), axis=1)
        return coords
    
    def get_single_location(self, name, selection_dict={}, **selection_kwargs) -> Path:
        r = self.get_locations(name, selection_dict, **selection_kwargs)
        if len(r.index) !=1:
            raise Exception("Problem")
        else:
            return r["location"].iat[0]
        
    def compute(self, name,  selection_dict={}, **selection_kwargs) -> pd.DataFrame:
        locs = self.get_locations(name, selection_dict, **selection_kwargs)
        if locs["location"].duplicated().any():
            raise Exception(f"Duplication problem\n{locs}")
        import tqdm.auto as tqdm
        for _, row in tqdm.tqdm(locs.iterrows(), desc=f"compute {name}", disable=len(locs.index) < 10, total = len(locs.index)):
            try:
                self.p.data[name].compute(row["location"], row.drop("location").to_dict())
            except Exception as e:
                e.add_note(f'During computation of {name}({row["location"]}, {row.drop("location").to_dict()})')
                raise 
        return locs
    
    def compute_unique(self, name,  selection_dict={}, **selection_kwargs) -> Path:
        r = self.compute(name,  selection_dict, **selection_kwargs)
        if len(r.index) != 1:
            raise Exception(f"Problem {len(r.index)}\n{r}")
        else:
            return r["location"].iat[0]
    def __str__(self):
        return (
            f'Coordinates:\n\t'+"\n\t".join([str(k) for k in self.coords.keys()]) +
            f'\nFiles:\n\t'+"\n\t".join([f'{k} {tuple(self.p.data[k].coords)}' for k in self.p.data.keys()]) 
        )
    
        # str() + "\n" + str(self.p.data.keys())

def default_saver(path: Path, o):
    import pickle
    with path.open("wb") as f:
        pickle.dump(o, f)

def safe_save(saver):
    import shutil
    def new_saver(path, o):
        tmp_path = Path(str(path) + ".tmp")
        tmp_path.parent.mkdir(exist_ok=True, parents=True)
        saver(tmp_path, o)
        shutil.move(tmp_path, path)
    return new_saver

def open_and_save(saver, mode):
    def new_saver(path, o):
        with path.open(mode) as f:
            saver(f, o)
    return new_saver

def cache(saver=default_saver, open=None, force_recompute=False):
    def decorator(f):
        @functools.wraps(f)
        def new_f(out_location, *args, **kwargs):
            if out_location.exists() and not force_recompute:
                return
            res = f(out_location, *args, **kwargs)
            if open is None:
                msaver= saver
            else:
                msaver = open_and_save(saver, open)
            safe_save(msaver)(out_location, res)
                
        return new_f
    return decorator

def singleglob(p: Path, *patterns, error_string='Found {n} candidates for pattern {patterns} in folder {p}', only_ok=False):
    all = [path for pat in patterns for path in p.glob(pat)]
    if only_ok:
        return len(all)==1
    if len(all) >1:
        raise FileNotFoundError(error_string.format(p=p, n=len(all), patterns=patterns))
    if len(all) ==0:
        raise FileNotFoundError(error_string.format(p=p, n=len(all), patterns=patterns))
    return all[0]