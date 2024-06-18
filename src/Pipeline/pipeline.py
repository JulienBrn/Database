from typing import Dict, Tuple, NamedTuple, List, Callable, Any, Protocol
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
import inspect

class CoordsProtocol(Protocol):
    def meth(self, coords: Dict[str, Any]) -> pd.DataFrame: ...

@dataclass
class Coords:
    dependencies: List[str]
    func: CoordsProtocol

class DataLocProtocol(Protocol):
    def meth(self, coords: Dict[str, Any]) -> Path | List[Any]: ...

class DataComputeProtocol(Protocol):
    def meth(self, coords: Dict[str, Any], input_locations: Dict[str, pd.DataFrame]) -> Path | List[Any]: ...

@dataclass
class Data:
    dependencies: List[str]
    location: Callable[[Dict[str, Any]], Path | List[Any]]

    inputs: List[str]
    computation: DataComputeProtocol

class Pipeline:
    coords: Dict[str, Coords]
    data: Dict[str, Data]

    def __init__(self):
        self.coords={}
        self.data={}

    def coord(self, name: str, depends_on: List[str] = None):
        def decorator(f):
            self.coords[name] = Coords(dependencies=depends_on, func=f)
            return f
        return decorator
    
    def autocoord(self, name: str = None):
        def decorator(f):
            nonlocal name
            depends_on = list(inspect.signature(f).parameters.keys())
            if name is None:
                name = f.__name__
            self.coords[name] = Coords(dependencies=depends_on, func=lambda coords: f(**coords))
            return f
        return decorator
    
    def get_coords(self, name):
        coord = self.coords[name]
        # print("entering")
        df = pd.DataFrame([[]])
        for d in coord.dependencies:
            # print(df)
            ddf = self.get_coords(d)
            on = set(df.columns).intersection(set(ddf.columns))
            df = df.merge(ddf, how="outer" if on else "cross", on=on if on else None)
        # print(df)
        # print("done d")
        if df.empty:
            return coord.func(coords={})
        res = []
        for _, row in df.iterrows():
            tmp = coord.func(coords=row.loc[coord.dependencies].to_dict())
            for c in row.index:
                tmp[c] = row.at[c]
            if not name in tmp.columns:
                raise Exception(f'{name} should appear in columns...')
            res.append(tmp)
        return pd.concat(res)

