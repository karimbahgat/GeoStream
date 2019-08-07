
import sqlite3
import math



def register_funcs(db):
    db.create_aggregate("stdev", 1, StdevFunc)
    #db.create_aggregate("majority", 1, StdevFunc)
    #db.create_aggregate("minority", 1, StdevFunc)
    #db.create_aggregate("first", 1, StdevFunc)
    #db.create_aggregate("last", 1, StdevFunc)




class Majority:
    # from https://stackoverflow.com/questions/2298339/standard-deviation-for-sqlite
    def __init__(self):
        kl

    def step(self, value):
        try:
            if value is None:
                return None

            
        except Exception as EXStep:
            pass
            return None    

    def finalize(self):
        try:
            

            return self.S
        except Exception as EXFinal:
            pass
            return None

class Minority:
    pass

class First:
    pass

class Last:
    pass

class StdevFunc:
    # from https://stackoverflow.com/questions/2298339/standard-deviation-for-sqlite
    def __init__(self):
        self.M = 0.0    #Mean
        self.V = 0.0    #Used to Calculate Variance
        self.S = 0.0    #Standard Deviation
        self.k = 1      #Population or Small 

    def step(self, value):
        try:
            if value is None:
                return None

            tM = self.M
            self.M += (value - tM) / self.k
            self.V += (value - tM) * (value - self.M)
            self.k += 1
        except Exception as EXStep:
            pass
            return None    

    def finalize(self):
        try:
            if ((self.k - 1) < 3):
                return None

            #Now with our range Calculated, and Multiplied finish the Variance Calculation
            self.V = (self.V / (self.k-2))

            #Standard Deviation is the Square Root of Variance
            self.S = math.sqrt(self.V)

            return self.S
        except Exception as EXFinal:
            pass
            return None
        



