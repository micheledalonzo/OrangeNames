# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import sys
sys.path.append("c:/Orange/")
import sys
import inspect
import traceback
import linecache
import argparse
import pypyodbc
import datetime
import sqlite3
#from fuzzywuzzy import fuzz
#from fuzzywuzzy import process
from pattern.it import parse, split, parsetree
from pattern.it import pprint
from pattern.metrics import similarity, levenshtein
import time
import logging
INFO     = logging.INFO
CRITICAL = logging.CRITICAL
FATAL    = logging.FATAL
DEBUG    = logging.DEBUG
ERROR    = logging.ERROR
CRITICAL = logging.CRITICAL
WARN     = logging.WARN
WARNING  = logging.WARNING
NO = 0
me = "NAM"
DsnProd         = 'DSN=Orange'
restart         = False
DsnTest         = 'DSN=OrangeTest'
Dsn             = DsnTest
cSql            = None
cLite           = None

def CreateMemTableKeywords():
    try:
        cmd_create_table = """CREATE TABLE if not exists 
                  keywords (
                            assettype   STRING,
                            language    STRING,
                            keyword     STRING,
                            operatore   STRING,
                            tipologia1  STRING,
                            tipologia2  STRING,
                            tipologia3  STRING,
                            tipologia4  STRING,
                            tipologia5  STRING,
                            replacewith STRING,
                            numwords    INTEGER
        );"""
        SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        #log(ERROR, err)
        return False


def dbCreateMemTableKeywords():
    try:
        cmd_create_table = """CREATE TABLE if not exists 
                  keywords (
                            assettype   STRING,
                            language    STRING,
                            keyword     STRING,
                            operatore   STRING,
                            tipologia1  STRING,
                            tipologia2  STRING,
                            tipologia3  STRING,
                            tipologia4  STRING,
                            tipologia5  STRING,
                            replacewith STRING,
                            numwords    INTEGER
        );"""
        SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        log(ERROR, err)
        return False


def dbAAsset(Asset, AssetMatch, AssetRef):
    try:
        if AssetMatch == 0:   # devo inserire me stesso
            cSql.execute("select * from asset where asset = ?", ([Asset]))
             # inserisce asset con info standardizzate     
            cSql.execute("Insert into AAsset (Updated) values (?)" , ([RunDate]))
            cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            lstrec = cSql.fetchone()
            if lstrec is None:
                raise Exception("Errore get autonum")
            AAsset = int(lstrec[0])
            cSql.execute("Update Asset set AAsset=? where Asset=?", (AAsset, Asset))
        else:
            AAsset = AssetRef
            cSql.execute("Update Asset set AAsset=? where Asset=?", (AssetRef, Asset))  # ci metto il record di rif 
        
        return AAsset

    except Exception as err:
        log(ERROR, err)
        return False

def CopyAssetInMemory():
    
    try:
        log(INFO, "Loading assets....")
        cSql.execute("Select * from QAddress order by Name")
        memassets = cSql.fetchall()
        count = 0
        for asset in memassets:
            count = count + 1
            AAsset      = asset['aasset']
            Asset       = asset['asset']
            Country     = asset['country']
            Source      = asset['source']
            Name        = asset['name']
            NameSimple  = asset['namesimple']
            NameSimplified = asset['namesimplified']
            AddrStreet  = asset['addrstreet']
            AddrCity    = asset['addrcity']
            AddrZIP     = asset['addrzip']
            AddrCounty  = asset['addrcounty']
            AddrPhone   = asset['addrphone']
            AddrWebsite = asset['addrwebsite']
            Assettype   = asset['assettype']
            AddrRegion  = asset['addrregion']
            FormattedAddress =  asset['formattedaddress']
            cLite.execute("insert into MemAsset \
                            (aasset, asset, assettype, country, name, namesimple, namesimplified, addrstreet, addrcity, addrzip, addrcounty, addrphone, addrwebsite, addrregion, formattedaddress, source) \
                                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (AAsset, Asset, Assettype, Country, Name, NameSimple, NameSimplified, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress, Source))
        log(INFO, str(count) + " asset in memory")
        return True
    except Exception as err:
        log(ERROR, err)
        return False

def CopyKeywordsInMemory():
    
    cSql.execute("Select * from T_Tag order by keyword")
    ks = cSql.fetchall()
    for k in ks:
        assettype   = k['assettype']
        language    = k['language']
        keyword     = k['keyword']
        operatore   = k['operatore']
        tipologia1  = k['tipologia1']
        tipologia2  = k['tipologia2']
        tipologia3  = k['tipologia3']
        tipologia4  = k['tipologia4']
        tipologia5  = k['tipologia5']
        replacewith = k['replacewith']
        kwdnumwords = k['kwdnumwords']
        numwords    = len(keyword.split())
        cLite.execute("insert into keywords (assettype, language, keyword, operatore,tipologia1,tipologia2,replacewith,numwords) values (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (assettype, language, keyword, operatore,tipologia1,tipologia2,replacewith,numwords))
    return

def ParseArgs():
    testrun = debug = resetnames = False
    Dsn = ''
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', action='store_true', default=False,
                    dest='test',
                    help='Decide se il run e di test')
    parser.add_argument('-debug', action='store_true', default='',
                    dest='debug',
                    help="Dump tabelle interne su Db")
    parser.add_argument('-resetnames', action='store_true', default='',
                    dest='resetnames',
                    help="Inizializza tutti i nomi standard prima di una nuova standardizzazione dei nomi. Esclusi i nomi modificati a mano")

    args = parser.parse_args()
    if args.test:
        testrun = True
        Dsn = DsnTest
        print("RUN DI TEST!!!!")
    else:
        testrun = False
        Dsn = DsnProd
        print("RUN EFFETTIVO")
    if args.debug:
        debug = True
    if args.resetnames:
        resetnames = True
    
    Args = args

    return testrun, Dsn, debug, resetnames

def RunIdCreate(RunType):
    try:
        runid = 0
        cSql.execute("Insert into Run (Start, RunType) Values (?, ?)", (str(datetime.datetime.now().replace(microsecond = 0)), RunType))
        cSql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
        run = cSql.fetchone()
        if run is None:
            raise Exception("Get autonum generato con errore")
        runid = run[0]    
        return runid
    except Exception as err:        
        #log(ERROR, err)
        return False

def dbAssetTag(Asset, Ttag, tagname):
     
    try:
        # cancella e riscrive la classificazione dell'asset     
        if len(Ttag)>0:
            Ttag = list(set(Ttag))     # rimuovo duplicati dalla lista        
            #cSql.execute("Delete * from AssetTag where Asset = ? and TagName = ?", (Asset, tagname))
            for i in Ttag:
                i = StdCar(i)
                if len(i) < 2:
                    continue
                cSql.execute("Select * from AssetTag where Asset=? and TagName=? and Tag=?", (Asset, tagname, i))
                a = cSql.fetchone()
                if a is None:
                    cSql.execute("Insert into AssetTag(Asset, TagName, Tag) Values (?, ?, ?)", (Asset, tagname, i))

        return True

    except Exception as err:        
        #log(ERROR, err)
        return False

def SetLogger(Typ, RunId, restart):    
    
    logger = logging.getLogger()  # root logger
    if len (logger.handlers) > 0:  # remove all old handlers        
        logger.handlers = []
    
    logger.setLevel(logging.DEBUG)   # default level
     
    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
 
    # create error file handler and set level to error
    if restart:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+'.err','a', encoding=None, delay="true")
    else:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+'.err','w', encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
 
    # create debug file handler and set level to debug
    if restart:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+".log","w")
    else:
        handler = logging.FileHandler("C:\\Orange\\Log\\"+Typ+"-"+str(RunId)+".log","a")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(levelname)-8s] [%(asctime)s] [%(message)s]', "%d-%m %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    log(INFO, 'INIZIO DEL RUN')

    return True

def log(level, *message):
    runmsg = ''
    logger = logging.getLogger()
    if level == DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.debug(runmsg)
        for msg in message:        
            runmsg = "--> %s" % (msg) 
            logger.debug(runmsg)    
        #logging.debug(stack_trace[:-1])

    if level == INFO:
        for msg in message:        
            runmsg = "%s" % (msg) 
            logger.info(runmsg)    

    if level == WARNING or level == WARN:
        for msg in message:        
            runmsg = "%s" % (msg) 
            logger.warn(runmsg)
    
    if level == ERROR or level == CRITICAL or level == FATAL:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        runmsg = "--> %s" % (inspect.stack()[1][3])   # nome della funzione
        logger.error(runmsg)
        for msg in message:        
            runmsg = "--> %s" % (msg) 
            logger.error(runmsg)    
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if exc_type is not None:
            filename = exc_traceback.tb_frame.f_code.co_filename
            lineno = exc_traceback.tb_lineno
            line = linecache.getline(filename, lineno)
            logger.error("--> Riga:%d - %s" % (lineno, line.strip()))
            #for line in pprint.pformat(stack_trace[:-1]).split('\n'):
            for line in stack_trace:
                logging.error(line.replace("\n",""))

def InsertWTag(nomeoriginale, keyword, lunghezza, tag):
    cSql.execute("select * from W_Tag where nomeoriginale = ? and  keyword = ? and pos = ?", (nomeoriginale, keyword, tag))
    a = cSql.fetchone()
    if a is None:
        # inserisce asset con info standardizzate     
        cSql.execute("Insert into W_Tag (nomeoriginale, keyword, lunghezza, pos) values (?, ?, ?, ?)" , (nomeoriginale, keyword, lunghezza, tag))

    return

def InsertTag(keyword, tag):
    cSql.execute("select * from T_Tag where keyword = ? and pos = ?", (keyword, tag))
    a = cSql.fetchone()
    if a is None:
        # inserisce asset con info standardizzate     
        cSql.execute("Insert into T_Tag (assettype, language, keyword, pos) values (?, ?,?,?)" , (1, 'ITA', keyword, tag))

    return


def controlla(name, frasecompleta, lunghezza):

    if len(name) == len(frasecompleta):
        return False
    if lunghezza > 6:
        return True
    # se ci sono queste parole, correggo
    for w in frasecompleta.split():
        if w.lower() == "&"         or \
           w.lower() == "sas"       or \
           w.lower() == "s.n.c."    or \
           w.lower() == "snc"       or \
           w.lower() == "srl"       or \
           w.lower() == "s.r.l.":
            return True
        # se ci sono questi nomi non correggo
        if w.lower() == "biancaneve"         or \
           w.lower() == "castrocaro":         
            return False
    
    return False

def LoadCustomTagging():    
    #import nltk.tag, nltk.data
    #defaulW_Tagger = nltk.data.load(nltk.tag._POS_TAGGER)
    #cSql.execute("select * from T_CustomTag where nomeoriginale = ? and  keyword = ? and pos = ?", (nomeoriginale, keyword, tag))
    #a = cSql.fetchone()

    #model = {'select': 'VB'}
    #tagger = nltk.tag.UnigramTagger(model=model, backoff=defaulW_Tagger)
    return

def Main():
    try:
        global cSql, cLite, SqLite, MySql
        testrun = Dsn = debug = resetnames = ''
        testrun, Dsn, debug, resetnames = ParseArgs()
        # apri connessione e cursori, carica keywords in memoria
        MySql = pypyodbc.connect(Dsn)
        cSql = MySql.cursor()
        SqLite = sqlite3.connect(':memory:')
        cLite = SqLite.cursor()
        RunId = RunIdCreate(me)
        rc = SetLogger(me, RunId, restart)      
        if not rc:
            log(ERROR, "SetLogger errato")
     
        #log(INFO, Args)
        N_Ass = 0

        if resetnames == True:
            cSql.execute("Delete * from W_Tag")

        #if debug:
        #   cSql.execute("Delete from Debug_Names")
 
        # creo la tabella in memoria
        rc = CreateMemTableKeywords()
        rc = CopyKeywordsInMemory()
        # seleziono le righe da esaminare (aggiungere restart?)
        cSql.execute("Select * from QAddress")
        rows = cSql.fetchall()
        T_Ass = len(rows)
        msg=('RUN %s: NAMES, %s Assets' % (RunId, T_Ass))
        log(INFO, msg)
        t1 = time.clock()
        for row in rows:
            Ttag = []
            cuc = []
            asset = row['asset']
            name = row['name']
            namesimple = row['namesimple']
            city = row['addrcity']
            assettype = row['assettype']
            country = row['country']
            lang = row['countrylanguage']
            fix = row['namedonottouch']

            if namesimple == None or namesimple == '' or namesimple.isspace(): # se simple e' vuoto ci copio il nome
                simplename = name.title()
                cSql.execute("Update Asset set NameSimple = ?, NameSimplified = ? where Asset = ?", (simplename, NO, asset))
                cSql.commit()
            if fix == 0: 
                frase = []
                ce = False
                msg = (str(N_Ass) + "(" + str(T_Ass) +") - " + name )
                log(INFO, msg)
                s = parsetree(name) 
                if len(s.words) == 1:   # c'e' solo una parola, non viene trattato
                    continue
                for word in s.words:
                    InsertTag(word.string, word.tag)
                    cSql.commit()
                for sentence in s.sentences:
                    for chunk in sentence.pnp:   # prepositional noun phrase
                        ce = False
                        #for tt in chunk.tagged:
                        if chunk.tagged[0][1] == 'IN' and \
                          (chunk.tagged[0][0] == 'Di' or chunk.tagged[0][0] == 'di'):  # se trovo una preposizione che inizia con "Di"
                            ce = True
                            cenomeproprio = False
                            for word in chunk.words: 
                                if word.tag == "NNP" or word.tag == "NNPS":
                                    cenomeproprio = True
                                frase.append(word.string)
                            if cenomeproprio:
                                cenomeproprio = False
                                frasecompleta = ' '.join(frase)
                                rc = controlla(name, frasecompleta, len(frase))
                                InsertWTag(name, frasecompleta, len(frase), "YYY")
                                cSql.commit()
                            frase = []
                            ce = False

                    #if debug:
                    #    rc = DumpNames(asset, name, simplename)
                    #rc = dbAssetTag(asset, tag, "Tipologia")
                    #rc = dbAssetTag(asset, cuc, "Cucina")
            N_Ass = N_Ass + 1
        t2 = time.clock()
        print(round(t2-t1, 3))
        # chiudi DB
        MySql.close()
        SqLite.close()

    except Exception as err:
        log(ERROR, err)
        return False

if __name__ == "__main__":       
    rc = Main()
    if not rc:
        #log(ERROR, "Run terminato in modo errato")        
        sys.exit(12)
    else:
        #log(INFO, "Run terminato in modo corretto")        
        sys.exit(0)



#print similarity("All'Osteria De Massimo Bevessimo E Magnassimo", "L'Osteria Del Tempo Perso", metric="dice")
#print similarity("All'Osteria De Massimo Bevessimo E Magnassimo", "L'Osteria Del Tempo Perso")
#print levenshtein("All'Osteria De Massimo Bevessimo E Magnassimo", "L'Osteria Del Tempo Perso")
#print levenshtein("L'Osteria Del Tempo Perso", "L'Osteria Del Tempo Perso")
#print levenshtein("L'Osteria Del Tempo Perso", "Bernengond")
#Keyword
# punteggiatura da trattare...
#-
#!
#"
#,
#:
#.
#;
#(
#)