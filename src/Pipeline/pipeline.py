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
    def decl_decorator(func: Callable, coords: set[str] = None):
        if coords is None:
            coords = set([func.__name__])
        func_args = set(inspect.signature(func).parameters.keys())
        def new_func(*args, **kwargs):
            res = func(*args, **kwargs)
            if not isinstance(res, pd.DataFrame):
                res = pd.DataFrame(res, columns=list(coords))
            if set(res.columns) != set(coords):
                raise Exception("Problem in coord definition")
            return res
        return CoordComputer(coords=set(coords), dependencies=func_args, compute=lambda d: new_func(**{k: v for k,v in d.items() if k in func_args}))


            
@dataclass
class Data:
    name: str
    coords: set[str]
    get_location: Callable[[Mapping[str, Any]], Any]
    compute: ComputeProtocol | None

    @staticmethod
    def decl_decorator(cls):
        if not hasattr(cls, "location"):
            raise Exception("No location function")
        loc_func = getattr(cls, "location")
        loc_func_args = set(inspect.signature(loc_func).parameters.keys())
        compute_func = None if not hasattr(cls, "compute") else getattr(cls, "compute")
        name = getattr(cls, "name") if hasattr(cls, "name") else cls.__name__ 
        return Data(name=name,coords=loc_func_args, get_location=lambda d: loc_func(**{k: v for k,v in d.items() if k in loc_func_args}), compute=compute_func)

class Pipeline:
    coord_computers = List[CoordComputer]
    data: Dict[str, Data]

    def __init__(self):
        self.coord_computers = []
        self.data = {}



    def declare_coord(self, o: CoordComputer):
        self.coord_computers.append(o)
            

    def declare_data(self, d: Data):
        if d.name in self.data:
            raise Exception(f"Data {d.name} already exists")
        self.data[d.name] = d


    def register_coord(self, coords=None):
        def decorator(f):
            self.declare_coord(CoordComputer.decl_decorator(f, coords))
            return f
        return decorator
    
    
    def register_data(self):
        def decorator(f):
            self.declare_data(Data.decl_decorator(f))
            return f
        return decorator
    
    def initialize(self):
        if hasattr(self, "instance"):
            raise Exception("Cannot be initialized twice")
        inst = PipelineInstance(self)
        self.instance = inst
        return inst
    
    def __getattr__(self, name: str) -> Any:
        if name != "instance":
            if hasattr(self, "instance"):
                return self.instance.__getattr__(name)
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
    

    def get_coords(self, names: set[str], selection_dict = {}, **selection_kwargs):
        selection_dict = selection_dict | selection_kwargs
        all_names = self._get_dependencies(names)
        
        df = pd.DataFrame([[]])
        while not all_names.issubset(set(df.columns)):
            for cc in self.p.coord_computers:
                if cc.coords.intersection(all_names) != set() and cc.dependencies.issubset(set(df.columns)) and not cc.coords.issubset(set(df.columns)):
                    tmp_dfs = []
                    dep_list = list(cc.dependencies)
                    if len(cc.dependencies) > 0:
                        for ind, _ in df.groupby(dep_list if len(dep_list) > 1 else dep_list[0]):
                            if not isinstance(ind, tuple):
                                ind = (ind, )
                            dep_dict = {k:v for k, v in zip(dep_list, ind)}
                            ret_df = cc.compute(dep_dict)
                            ret_df = self._filter(ret_df, selection_dict)
                            for k, v in dep_dict.items():
                                ret_df[k] = v
                            tmp_dfs.append(ret_df)
                        tmp_df = pd.concat(tmp_dfs)
                        df = pd.merge(df, tmp_df, on=list(cc.dependencies), how="inner") 
                    else:
                        ndf = cc.compute({})
                        ndf = self._filter(ndf, selection_dict)
                        df = pd.merge(df, ndf, how="cross") 
        res = df.groupby(list(all_names), group_keys=False).apply(lambda g: g.drop_duplicates(subset=list(all_names), inplace=False))
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
        
    def compute(self, name,  selection_dict={}, **selection_kwargs) -> None:
        locs = self.get_locations(name, selection_dict, **selection_kwargs)
        if locs["location"].duplicated().any():
            raise Exception(f"Duplication problem\n{locs}")
        for _, row in locs.iterrows():
            try:
                self.p.data[name].compute(row["location"], row.drop("location").to_dict())
            except Exception as e:
                e.add_note(f'During computation of {name}({row["location"]}, {row.drop("location").to_dict()})')
                raise 


# class CoordsProtocol(Protocol):
#     def meth(self, coords: Dict[str, Any]) -> pd.DataFrame: ...

# @dataclass
# class Coords:
#     dependencies: List[str]
#     func: CoordsProtocol

# class DataLocProtocol(Protocol):
#     def meth(self, coords: Dict[str, Any]) -> Path | List[Any]: ...

# class DataComputeProtocol(Protocol):
#     def meth(self, out_location, coords: Dict[str, Any]) -> None: ...

# @dataclass
# class Data:
#     dependencies: List[str]
#     location: DataLocProtocol
#     computation: DataComputeProtocol | None

# class Pipeline:
#     coords: Dict[str, Coords]
#     data: Dict[str, Data]

#     def __init__(self):
#         self.coords={}
#         self.data={}


#     def declare_coord(self, name: str, depends_on: List[str], func):
#         self.coords[name] = Coords(dependencies=depends_on, func=func)


#     def declare_data(self, name, depends_on: List[str], loc_func: DataLocProtocol, compute_func: DataComputeProtocol=None):
#         self.data[name] = Data(dependencies=depends_on, location=loc_func, computation=compute_func)

#     def register_coord(self, name: str = None, depends_on: List[str] = None):
#         def decorator(f):
#             nonlocal name, depends_on
#             if depends_on is None:
#                 depends_on = list(inspect.signature(f).parameters.keys())
#                 func = lambda coords: f(**{k: v for k,v in coords.items() if k in depends_on})
#             else:
#                 func = f
#             if name is None:
#                 name = f.__name__
#             self.declare_coord(name, depends_on=depends_on, func=func)
#             return f
#         return decorator
    
#     def register_data(self, name=None, depends_on=None):
#         def decorator(cls):
#             nonlocal name, depends_on
#             if name is None:
#                 if hasattr(cls, "name"):
#                     name = cls.name
#                 else:
#                     name = cls.__name__
#             if depends_on is None:
#                 depends_on = list(inspect.signature(cls.location).parameters.keys())
#                 for n in depends_on:
#                     if n in self.coords:
#                         for d in  self.coords[n].dependencies:
#                             if d in self.coords and not d in depends_on:
#                                 raise Exception(f"Problem {[d for d in self.coords[n].dependencies if d in self.coords]} {depends_on}")
#                 loc_func = lambda coords: cls.location(**{k: v for k,v in coords.items() if k in depends_on})
#                 compute_func =  lambda out_location, coords: cls.compute(out_location, **{k: v for k,v in coords.items() if k in depends_on}) if hasattr(cls, "compute") else None
#             else:
#                 loc_func = cls.location
#                 compute_func = cls.compute if hasattr(cls, "compute") else None
#             self.declare_data(name, depends_on, loc_func=loc_func, compute_func=compute_func)
#             return cls
#         return decorator


#     def get_coords(self, names, selection_dict = {}, **selection_kwargs):
#         selection_dict = selection_dict | selection_kwargs
#         if isinstance(names, str):
#             names = (names, )
#         df = pd.DataFrame([[]])
#         names= set(d for n in names for d in self.coords[n].dependencies if d in self.coords).union(set(names))
#         while True:
#             names = names.difference(set(df.columns))
#             name = [n for n in names if set(self.coords[n].dependencies).issubset(set(df.columns))]
#             if len(name)==0:
#                 break
#             else:
#                 name=name[0]
#             coord = self.coords[name]

#             r = []
#             for _, row in df.iterrows():
#                 tmp = coord.func(coords=row.to_dict())
#                 for c in row.index:
#                     tmp[c] = row.at[c]
#                 if not name in tmp.columns:
#                     raise Exception(f'{name} should appear in columns...')
#                 r.append(tmp)
#             ddf= pd.concat(r)
#             for s, v in selection_dict.items():
#                 if s in ddf.columns:
#                     if isinstance(v, tuple):
#                         ddf = ddf.loc[ddf[s].isin(v)]
#                     elif isinstance(v, slice):
#                         if v.step:
#                             Exception("step not handled")
#                         start_cond = ddf[s] >= v.start if v.start else True
#                         end_cond = ddf[s] < v.stop if v.stop else True
#                         ddf = ddf.loc[start_cond & end_cond]
#                     else:
#                         ddf = ddf.loc[ddf[s] ==v]
#             on = set(df.columns).intersection(set(ddf.columns))
#             df = df.merge(ddf, how="outer" if on else "cross", on=list(on) if on else None)
#         return df
    
#     def get_locations(self, name, selection_dict={}, **selection_kwargs) -> pd.Series:
#         coords = self.get_coords(self.data[name].dependencies, selection_dict, **selection_kwargs)
#         cols = [str(c) for c in coords.columns]
#         coords["location"] = coords.apply(lambda row: self.data[name].location(row.to_dict()), axis=1)
#         return coords.set_index(cols)["location"]
            

#     def get_single_location(self, name, selection_dict={}, **selection_kwargs) -> Path:
#         r = self.get_locations(name, selection_dict, **selection_kwargs)
#         if len(r.index) !=1:
#             raise Exception("Problem")
#         else:
#             return r.iat[0]
    
#     def compute(self, name,  selection_dict={}, **selection_kwargs) -> None:
#         locs = self.get_locations(name, selection_dict, **selection_kwargs).reset_index()
#         for _, row in locs.iterrows():
#             self.data[name].computation(row.iat[-1], row.iloc[:-1].to_dict())
