from typing import Dict, Tuple, NamedTuple, List, Callable, Any, Protocol
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
import inspect
import collections, collections.abc


class CoordsProtocol(Protocol):
    def meth(self, coords: Dict[str, Any]) -> pd.DataFrame: ...

@dataclass
class Coords:
    dependencies: List[str]
    func: CoordsProtocol

class DataLocProtocol(Protocol):
    def meth(self, coords: Dict[str, Any]) -> Path | List[Any]: ...

class DataComputeProtocol(Protocol):
    def meth(self, out_location, coords: Dict[str, Any]) -> None: ...

@dataclass
class Data:
    dependencies: List[str]
    location: DataLocProtocol
    computation: DataComputeProtocol | None

class Pipeline:
    coords: Dict[str, Coords]
    data: Dict[str, Data]

    def __init__(self):
        self.coords={}
        self.data={}


    def declare_coord(self, name: str, depends_on: List[str], func):
        self.coords[name] = Coords(dependencies=depends_on, func=func)


    def declare_data(self, name, depends_on: List[str], loc_func: DataLocProtocol, compute_func: DataComputeProtocol=None):
        self.data[name] = Data(dependencies=depends_on, location=loc_func, computation=compute_func)

    def register_coord(self, name: str = None, depends_on: List[str] = None):
        def decorator(f):
            nonlocal name, depends_on
            if depends_on is None:
                depends_on = list(inspect.signature(f).parameters.keys())
                func = lambda coords: f(**{k: v for k,v in coords.items() if k in depends_on})
            else:
                func = f
            if name is None:
                name = f.__name__
            self.declare_coord(name, depends_on=depends_on, func=func)
            return f
        return decorator
    
    def register_data(self, name=None, depends_on=None):
        def decorator(cls):
            nonlocal name, depends_on
            if name is None:
                if hasattr(cls, "name"):
                    name = cls.name
                else:
                    name = cls.__name__
            if depends_on is None:
                depends_on = list(inspect.signature(cls.location).parameters.keys())
                for n in depends_on:
                    if n in self.coords:
                        for d in  self.coords[n].dependencies:
                            if d in self.coords and not d in depends_on:
                                raise Exception(f"Problem {[d for d in self.coords[n].dependencies if d in self.coords]} {depends_on}")
                loc_func = lambda coords: cls.location(**{k: v for k,v in coords.items() if k in depends_on})
                compute_func =  lambda out_location, coords: cls.compute(out_location, **{k: v for k,v in coords.items() if k in depends_on}) if hasattr(cls, "compute") else None
            else:
                loc_func = cls.location
                compute_func = cls.compute if hasattr(cls, "compute") else None
            self.declare_data(name, depends_on, loc_func=loc_func, compute_func=compute_func)
            return cls
        return decorator


    def get_coords(self, names, selection_dict = {}, **selection_kwargs):
        selection_dict = selection_dict | selection_kwargs
        if isinstance(names, str):
            names = (names, )
        df = pd.DataFrame([[]])
        names= set(d for n in names for d in self.coords[n].dependencies if d in self.coords).union(set(names))
        while True:
            names = names.difference(set(df.columns))
            name = [n for n in names if set(self.coords[n].dependencies).issubset(set(df.columns))]
            if len(name)==0:
                break
            else:
                name=name[0]
            coord = self.coords[name]

            r = []
            for _, row in df.iterrows():
                tmp = coord.func(coords=row.to_dict())
                for c in row.index:
                    tmp[c] = row.at[c]
                if not name in tmp.columns:
                    raise Exception(f'{name} should appear in columns...')
                r.append(tmp)
            ddf= pd.concat(r)
            for s, v in selection_dict.items():
                if s in ddf.columns:
                    if isinstance(v, tuple):
                        ddf = ddf.loc[ddf[s].isin(v)]
                    elif isinstance(v, slice):
                        if v.step:
                            Exception("step not handled")
                        start_cond = ddf[s] >= v.start if v.start else True
                        end_cond = ddf[s] < v.stop if v.stop else True
                        ddf = ddf.loc[start_cond & end_cond]
                    else:
                        ddf = ddf.loc[ddf[s] ==v]
            on = set(df.columns).intersection(set(ddf.columns))
            df = df.merge(ddf, how="outer" if on else "cross", on=list(on) if on else None)
        return df
    
    def get_locations(self, name, selection_dict={}, **selection_kwargs) -> pd.Series:
        coords = self.get_coords(self.data[name].dependencies, selection_dict, **selection_kwargs)
        cols = [str(c) for c in coords.columns]
        coords["location"] = coords.apply(lambda row: self.data[name].location(row.to_dict()), axis=1)
        return coords.set_index(cols)["location"]
            

    def get_single_location(self, name, selection_dict={}, **selection_kwargs) -> Path:
        r = self.get_locations(name, selection_dict, **selection_kwargs)
        if len(r.index) !=1:
            raise Exception("Problem")
        else:
            return r.iat[0]
    
    def compute(self, name,  selection_dict={}, **selection_kwargs) -> None:
        locs = self.get_locations(name, selection_dict, **selection_kwargs).reset_index()
        for _, row in locs.iterrows():
            self.data[name].computation(row.iat[-1], row.iloc[:-1].to_dict())
