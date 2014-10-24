# -*- coding: cp1252 -*-.
# Created on 21/mag/2014
# @author: mda
#!/usr/local/bin/python
import sys
import string
import inspect
import traceback
import linecache
import argparse
import pypyodbc
import pymysql
import datetime
import sqlite3
import nltk 
from pattern.it import parse, split, parsetree
from pattern.it import pprint
from pattern.metrics import similarity, levenshtein
import time
import logging
import progressbar
from fuzzywuzzy import fuzz


INFO     = logging.INFO
CRITICAL = logging.CRITICAL
FATAL    = logging.FATAL
DEBUG    = logging.DEBUG
ERROR    = logging.ERROR
CRITICAL = logging.CRITICAL
WARN     = logging.WARN
WARNING  = logging.WARNING
NO = 0
YES = -1
DsnProd         = 'DSN=Orange'
restart         = False
DsnTest         = 'DSN=OrangeTest'
Dsn             = DsnTest
global cMsAcc, cLite, SqLite, MsAcc, xMySql, cMySql, MySql

def Std_CreateMemTableMemAsset():
    if trace: log(DEBUG)   
    try:
        cmd_create_table = """CREATE TABLE if not exists
                    MemAsset (
                                Asset            INTEGER,
                                Country          VARCHAR(255),
                                Aasset           INTEGER,
                                Name             VARCHAR(255),
                                Source           INTEGER,
                                NameSimple       VARCHAR(255),
                                Assettype        INTEGER,
                                AddrStreet       VARCHAR(255),
                                AddrCity         VARCHAR(255),
                                AddrZIP          VARCHAR(255),
                                AddrCounty       VARCHAR(255),
                                AddrPhone        VARCHAR(255),
                                AddrWebsite      VARCHAR(255),
                                AddrRegion       VARCHAR(255),
                                FormattedAddress VARCHAR(255),
                                Namesimplified   INTEGER
                            );"""
        SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        log(ERROR, err)
        return False

def Names_CreateMemTableKeywords():
    if trace: log(DEBUG)  
    try:
        cmd_create_table = """CREATE TABLE if not exists 
                  keywords (
                            assettype   STRING,
                            language    STRING,
                            keyword     STRING,
                            pos         STRING,
                            mypos       STRING,
                            tipologia1  STRING,
                            tipologia2  STRING,
                            tipologia3  STRING,
                            cucina1     STRING,
                            cucina2     STRING,
                            cucina3     STRING,
                            replacewith STRING                            
        );"""
        SqLite.executescript(cmd_create_table)
        return True
    except Exception as err:
        log(ERROR, err)
        return False

def Std_AAsset(Asset, AssetMatch, AssetRef):
    if trace: log(DEBUG)   
    try:
        if AssetMatch == 0:   # devo inserire me stesso
            cMySql.execute("select * from asset where asset = %s", ([Asset]))
            # inserisce asset con info standardizzate     
            wrk = datetime.datetime.now()
            now = str(wrk.replace(microsecond = 0))
            cMySql.execute("Insert into AAsset (Updated) values (%s)" , ([now]))
            #cMySql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
            AAsset = cMySql.lastrowid  # recupera id autonum generato
            #lstrec = cMySql.fetchone()
            if AAsset is None:
                raise Exception("Errore get autonum")
            #AAsset = int(lstrec[0])
            cMySql.execute("Update Asset set AAsset=%s where Asset=%s", (AAsset, Asset))
            cLite.execute("Update MemAsset set AAsset=? where Asset=?", (AAsset, Asset))
        else:
            AAsset = AssetRef
            cMySql.execute("Update Asset set AAsset=%s where Asset=%s", (AssetRef, Asset))  # ci metto il record di rif 
            cLite.execute("Update MemAsset set AAsset=? where Asset=?", (AssetRef, Asset))     # aggiorno anche la tabella in memoria
        
        return AAsset

    except Exception as err:
        log(ERROR, err)
        return False

def Std_CopyAssetInMemory():
    global memassets

    if trace: log(DEBUG)   
    try:
        log(INFO, "Loading assets....")
        cMySql.execute("Select * from QAddress order by Name")
        #cMySql.execute("Select * from QAddress order by Name")
        memassets = cMySql.fetchall()
        count = 0
        for asset in memassets:
            count = count + 1
            AAsset      = asset['AAsset']
            Asset       = asset['Asset']
            Country     = asset['Country']
            Source      = asset['Source']
            Name        = asset['Name']
            NameSimple  = asset['NameSimple']
            NameSimplified = asset['NameSimplified']
            AddrStreet  = asset['AddrStreet']
            AddrCity    = asset['AddrCity']
            AddrZIP     = asset['AddrZIP']
            AddrCounty  = asset['AddrCounty']
            AddrPhone   = asset['AddrPhone']
            AddrWebsite = asset['AddrWebsite']
            Assettype   = asset['AssetType']
            AddrRegion  = asset['AddrRegion']
            FormattedAddress =  asset['FormattedAddress']
            cLite.execute("insert into MemAsset \
                            (aasset, asset, assettype, country, name, namesimple, namesimplified, addrstreet, addrcity, addrzip, addrcounty, addrphone, addrwebsite, addrregion, formattedaddress, source) \
                                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (AAsset, Asset, Assettype, Country, Name, NameSimple, NameSimplified, AddrStreet, AddrCity, AddrZIP, AddrCounty, AddrPhone, AddrWebsite, AddrRegion, FormattedAddress, Source))
        log(INFO, str(count) + " asset in memory")
        return True
    except Exception as err:
        log(ERROR, err)
        return False


def ParseArgs():

    testrun = debug = nomi = genera = trace = std = False
    Dsn = ''
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', action='store_true', default=False,
                    dest='test',
                    help='Decide se il run e di test')
    parser.add_argument('-std', action='store_true', default=False,
                    dest='std',
                    help='Unisce asset identici di fonti diverse')
    parser.add_argument('-debug', action='store_true', default=False,
                    dest='debug',
                    help="Dump tabelle interne su Db")
    parser.add_argument('-trace', action='store_true', default=False,
                    dest='trace',
                    help="Trace funzioni")
    parser.add_argument('-nomi', action='store_true', default=False,
                    dest='nomi',
                    help="Semplifica i nomi degli asset")
    parser.add_argument('-genera', action='store_true', default=False,
                    dest='genera',
                    help="Genera/aggiorna la tabella dei nomi degli asset")

    args = parser.parse_args()
    if args.test:
        testrun = True
        Dsn = DsnTest
        print("RUN DI TEST!!!!")
    else:
        testrun = False
        Dsn = DsnProd
        print("RUN EFFETTIVO")
    if args.std:
        std = True
    if args.debug:
        debug = True
    if args.trace:
        trace= True
    if args.genera:
        genera = True
    if args.nomi:
        nomi = True
   
    Args = args

    return testrun, Dsn, debug, genera, nomi, trace, std

def RunIdCreate(RunType):
    if trace: log(DEBUG)   
    try:
        runid = 0
        cMySql.execute("Insert into Run (Start, RunType) Values (%s, %s)", (str(datetime.datetime.now().replace(microsecond = 0)), RunType))
        #cMySql.execute("SELECT @@IDENTITY")  # recupera id autonum generato
        runid = cMySql.lastrowid
        if runid is None:
            raise Exception("Get autonum generato con errore")
        #runid = run[0]    
        return runid
    except Exception as err:        
        log(ERROR, err)
        return False

def AssetTag(Asset, Ttag, tagname):
    if trace: log(DEBUG)   
    try:
        # cancella e riscrive la classificazione dell'asset     
        if len(Ttag)>0:
            Ttag = list(set(Ttag))     # rimuovo duplicati dalla lista        
            #cMsAcc.execute("Delete * from AssetTag where Asset = ? and TagName = ?", (Asset, tagname))
            for i in Ttag:
                i = StdCar(i)
                if len(i) < 2:
                    continue
                cMyCql.execute("Select * from AssetTag where Asset=%s and TagName=%s and Tag=%s", (Asset, tagname, i))
                a = cMyCql.fetchone()
                if a is None:
                    cMyCql.execute("Insert into AssetTag(Asset, TagName, Tag) Values (%s, %s, %s)", (Asset, tagname, i))

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

def Genera_InsertTPosFrasi(nomeoriginale, keyword, lunghezza, tag):
    try:
        if trace: log(DEBUG)   
        cMySql.execute("select * from t_pos_frasi where nomeoriginale = %s and  frase = %s and mypos = %s", (nomeoriginale, keyword, tag))
        a = cMySql.fetchone()
        if a is None:
            # inserisce asset con info standardizzate     
            cMySql.execute("Insert into t_pos_frasi (nomeoriginale, frase, lunghezza, mypos) values (%s, %s, %s, %s)" , (nomeoriginale, keyword, lunghezza, tag))
        return
    except Exception as err:
        log(ERROR, err)
        return False

def Genera_InsertTag(keyword, tag):
    try:
        if trace: log(DEBUG)   
        cMySql.execute("select * from T_Pos where keyword = %s", ([keyword]))
        a = cMySql.fetchone()
        if a is None:
            # inserisce asset con info standardizzate     
            cMySql.execute("Insert into T_Pos (assettype, language, keyword, pos) values (%s, %s,%s,%s)" , (1, 'ITA', keyword, tag))
        return
    except Exception as err:
        log(ERROR, err)
        return False



def Genera_Controlla(name, frasecompleta, lunghezza):
    try:
        if trace: log(DEBUG)   
        if len(name) == len(frasecompleta):
            return False
        if lunghezza < 4:
            return False
        if lunghezza > 6:
            return True
        # se ci sono queste parole, correggo
        for w in frasecompleta.split():
            if w.lower() == "&"         or \
               w.lower() == "sas"       or \
               w.lower() == "c"         or \
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

    except Exception as err:
        log(ERROR, err)
        return False



def Names_Main():
    global Frasi
    me = "NAM"
    # legge tutti i nomi ed e li semplifica 
    try:
        if trace: log(DEBUG)   
        tagger = None
        RunId = RunIdCreate(me)
        rc = SetLogger(me, RunId, restart)      
        if not rc:
            log(ERROR, "SetLogger errato")
     
        #log(INFO, Args)
        N_Ass = 0

        msg=('RUN %s: NAMES, reading...' % (RunId))
        log(INFO, msg)
        # creo la tabella in memoria
        rc = Names_CreateMemTableKeywords()
        tagger = Names_LoadCustomTagging()  # carica le keywords da trattare
        Frasi  = Names_LoadFrasi()

        # seleziono le righe da esaminare (aggiungere restart?)
        cMySql.execute("Select asset, name, assettype, country, countrylanguage from QAddress where NameDoNotTouch = 0")
        rows = cMySql.fetchall()
        T_Ass = len(rows)
        msg=('RUN %s: NAMES, %s Assets' % (RunId, T_Ass))
        log(INFO, msg)
        t1 = time.clock()
        bar = progressbar.ProgressBar(maxval=T_Ass,widgets=[progressbar.Bar('*', '[', ']'), ' ', progressbar.Percentage()])
        for row in rows:
            Ttag = []
            cuc = []
            asset = row['asset']
            name = row['name']
            assettype = row['assettype']
            country = row['country']
            lang = row['countrylanguage']
            name = Names_DeleteFrase(name)
            simplename = Names_Change(tagger, asset, name, assettype, lang) 
            if not simplename:
               log(ERROR, "Errore in NamesChange" + name)
               continue
            if simplename != name:
               log(WARNING, name+"---"+simplename)
               cMySql.execute("Update Asset set NameSimple = %s, NameSimplified = 1 where Asset = %s", (simplename, asset))
               if debug: 
                   Names_Dump(asset, name, simplename)
                   cMsAcc.commit()
            N_Ass = N_Ass + 1
            bar.update(N_Ass)
        t2 = time.clock()
        print(round(t2-t1, 3))
        bar.finish()
        return True
    
    except Exception as err:
        log(ERROR, err)
        return False

def Names_LoadFrasi():
    try:
        cMySql.execute("select * from T_Pos_Frasi where mypos is not null ")
        rows = cMySql.fetchall()
        return rows
    except Exception as err:
        log(ERROR, err)
        return False

def Names_DeleteFrase(name):
    try:
        for item in Frasi:
            if item['Frase'] in name:
                return name.replace(item['Frase'], "")
        return name

    except Exception as err:
        log(ERROR, err)
        return False


def Names_LoadCustomTagging():    
    try:
        if trace: log(DEBUG)   
        import nltk.tag, nltk.data
        model = {}
        default_Wtagger = nltk.data.load(nltk.tag._POS_TAGGER)
        cMySql.execute("select * from T_Pos where  mypos <> ''")
        rows = cMySql.fetchall()
        for row in rows:
            assettype   = row['AssetType']
            language    = row['Language']
            keyword     = row['KeyWord']
            pos         = row['Pos']
            mypos       = row['MyPos']
            tipologia1  = row['Tipologia1']
            tipologia2  = row['Tipologia2']
            tipologia3  = row['Tipologia3']
            cucina1     = row['Cucina1']
            cucina2     = row['Cucina2']
            cucina3     = row['Cucina3']
            replacewith = row['ReplaceWith']

            cLite.execute("insert into keywords (assettype, language, keyword, pos, mypos, tipologia1, tipologia2, tipologia2, cucina1, cucina2, cucina3, replacewith) \
                                                 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                                (assettype, language, keyword, pos, mypos, tipologia1, tipologia2, tipologia2, cucina1, cucina2, cucina3, replacewith))

            model[keyword] = mypos   # costruisci il tagger per classificare le parole da trattare

        tagger = nltk.tag.UnigramTagger(model=model, backoff=default_Wtagger)
        return tagger

    except Exception as err:
        log(ERROR, err)
        return False

def Names_Stdze(name):
    if trace: log(DEBUG)   
    try:
        #todelete = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~‘’“”–'
        todelete = '!"#$%()*+,-./:;<=>@[\\]^_`{|}~'
        frstlst  = "\'"
        stdnam = []; n = 0; 

        for idx, i in enumerate(name):
            # se primo e ultimo carattere sono inutili li tolgo
            #if name[0] in frtlst and name[len(stdnam)-1] in frtlst: 
            #    if idx == 0: continue
            #    if idx == len(name)-1: continue
            if i not in todelete:
                stdnam.append(i)
                n = 1                  
        # se primo e ultimo carattere sono inutili li tolgo
        while True:
            if stdnam[0] in frstlst and stdnam[len(stdnam)-1] in frstlst: 
                del stdnam[0]
                del stdnam[len(stdnam)-1]
            else:
                break

        if n == 1: 
            newnam = "".join(stdnam)
        else:
            newnam = "-"
        return newnam

    except Exception as err:
        log(ERROR, err)
        return False

def Names_Change(tagger, asset, name, assettype, lang):
    if trace: log(DEBUG)   
    try:
        if len(name) == 0:
            return "-"
        togli = '!"#$\'()*+,-./:;<=>?@[\\]^_`{|}~'
        typ     = []
        newname = []
        cuc     = []
        stdname = Names_Stdze(name)
        if not stdname:
            log(ERROR, "Error in stdze " + name)
            return False

        t = nltk.word_tokenize(stdname)
        n = tagger.tag(t)
        for idx, i in enumerate(n):
            word = i[0]
            ctag = i[1]
            if idx == len(n) - 1 and ctag == 'CC':  # se l'ultima parola è una congiunzione non la copio
                continue    

            if ctag == 'DEL' or ctag == 'RPL':
                cLite.execute("SELECT * from keywords where keyword = ? and assettype = ? and language = ?", (word, assettype, lang))
                check = cLite.fetchone()
                if check is None:
                    msg = "Kwd %s non trovata in tabella" % (i[1])
                    log(ERROR, msg)
                    continue
                xtyp1       = check[5]
                xtyp2       = check[6]
                xtyp3       = check[7]
                xcuc1       = check[8]
                xcuc2       = check[9]
                xcuc3       = check[10]
                replacew    = check[11]
                if xtyp1 is not None:
                    typ.append(xtyp1)
                if xtyp2 is not None:
                    typ.append(xtyp2)
                if xtyp3 is not None:
                    typ.append(xtyp3)
                if xcuc1 is not None:
                    cuc.append(xcuc1)
                if xcuc2 is not None:
                    cuc.append(xcuc2)
                if xcuc3 is not None:
                    cuc.append(xcuc3)
                continue
            if ctag == 'DEL':
                continue
            elif ctag == 'RPL':
                newname.append(replacew)
            elif ctag not in togli: 
                newname.append(word)  
        rc = AssetTag(asset, typ, "Tipologia")
        rc = AssetTag(asset, cuc, "Cucina")
        if len(newname) == 0:
            return "-"

        return " ".join(newname)

    except Exception as err:
        log(ERROR, err)
        return False



def Genera_ExtractName():
    if trace: log(DEBUG)   

    # legge tutti i nomi ed estrae tutte le keyword, le registra nella tabella T_Pos, che viene poi
    # letta per trattare opportunamente i nomi
    # da eseguirsi una tantum
    me = "GEN"
    try:
        RunId = RunIdCreate(me)
        rc = SetLogger(me, RunId, restart)      
        if not rc:
            log(ERROR, "SetLogger errato")
     
        #log(INFO, Args)
        N_Ass = 0

        # creo la tabella in memoria
        rc = Names_CreateMemTableKeywords()

        # seleziono le righe da esaminare (aggiungere restart?)
        msg=('RUN %s: NAMES, reading...' % (RunId))
        log(INFO, msg)
        cMySql.execute("Select * from QAddress")
        rows = cMySql.fetchall()
        T_Ass = len(rows)
        msg=('RUN %s: NAMES, %s Assets' % (RunId, T_Ass))
        log(INFO, msg)
        t1 = time.clock()
        bar = progressbar.ProgressBar(maxval=T_Ass,widgets=[progressbar.Bar('*', '[', ']'), ' ', progressbar.Percentage()])
        for row in rows:
            Ttag = []
            cuc = []
            asset = row['Asset']
            name = row['Name']
            namesimple = row['NameSimple']
            city = row['AddrCity']
            assettype = row['AssetType']
            country = row['Country']
            lang = row['CountryLanguage']
            fix = row['NameDoNotTouch']
            bar.update(N_Ass)
            if namesimple == None or namesimple == '' or namesimple.isspace(): # se simple e' vuoto ci copio il nome
                simplename = name.title()
                cMySql.execute("Update Asset set NameSimple = %s, NameSimplified = %s where Asset = %s", (simplename, NO, asset))
                
            if fix == 0: 
                frase = []
                ce = False
                msg = (str(N_Ass) + "(" + str(T_Ass) +") - " + name )
                log(DEBUG, msg)
                s = parsetree(name) 
                if len(s.words) == 1:   # c'e' solo una parola, non viene trattato
                    continue
                for word in s.words:
                    if word.string not in string.punctuation:
                        Genera_InsertTag(word.string, word.tag)
                        #cMySql.commit()
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
                                rc = Genera_Controlla(name, frasecompleta, len(frase))
                                if rc:  
                                    # inserisce la frase che potrebbe essere cancellata
                                    Genera_InsertTPosFrasi(name, frasecompleta, len(frase), "DEL")                                      
                                    MySql.commit()
                            frase = []
                            ce = False
            N_Ass = N_Ass + 1
        t2 = time.clock()
        bar.finish()
        print(round(t2-t1, 3))

        return True

    except Exception as err:
        log(ERROR, err)
        return False


def Names_Dump(Asset, name, NameSimple):
    if trace: log(DEBUG)   
    try:
        cMsAcc.execute("Delete from Debug_Names where Asset = ?", ([Asset]))             
        cMsAcc.execute("Insert into Debug_Names(Asset, Name, Newname) \
                         Values (?, ?, ?)", \
                       ( Asset, name, NameSimple))
    except Exception as err:
        log(ERROR, err)
        return False

    return True

def Std_DumpTabratio(tabratio):
    if trace: log(DEBUG)   
    if len(tabratio) == 0:
        return
    for item in tabratio:\
        #tabratio.append((gblratio, asset['name'], rows[j]['name'], rows[j]['asset'], rows[j]['aasset'], nameratio, streetratio, cityratio, zipratio, webratio, phoneratio ))                  
        cMsAcc.execute("Delete from Debug_TabRatio where Asset = ?", ([item[1]])) 
        break
    for item in tabratio:        
        cMsAcc.execute( "Insert into Debug_TabRatio(Asset, Assetref, AAssetref, Name, Nameref, Gblratio, Nameratio, Streetratio, Cityratio, Zipratio, Webratio, Phoneratio, Nameratio_ratio, Nameratio_partial, Nameratio_set) \
                          Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
                         (item[1], item[4],item[5],item[2],item[3],item[0],item[6],item[7],item[8],item[9],item[10],item[11],item[12],item[13],item[14]))                            

    return

def Std_Main():
    
    me = "STD"
    try:
        RunId = RunIdCreate(me)
        rc = SetLogger(me, RunId, restart)      
        if not rc:
            log(ERROR, "SetLogger errato")
     
        N_Ass = 0

        # creo la tabella in memoria
        rc = Std_CreateMemTableMemAsset()
        if not rc:
            raise Exception, "Non va"
        rc = Std_CopyAssetInMemory()
        cLite.execute("select * from MemAsset where AAsset <> 0")
        todolist = cLite.fetchall()     
        T_Ass = len(todolist)
        msg=('RUN %s: STDIZE %s Assets' % (RunId, T_Ass))
        log(INFO, msg)
        t1 = time.clock()
        for row in memassets:
            if row['AAsset'] > 0:   # già battezzato
                continue
            N_Ass = N_Ass + 1
            Asset = row['Asset']
            
            #if row['Asset'] != 3351: continue
            
            # "ALL" rifai tutti daccapo
            msg=('Asset %s - %s(%s)' % (Asset, N_Ass, T_Ass))
            log(INFO, msg)
            AssetMatch, AssetRef = Std_Asset(row, "ALL") 
            if AssetMatch is False: # is evita che 0 sia interpretato come false
                log(WARNING, "Asset " + str(Asset) + str(AssetMatch) + str(AssetRef))
                continue
            # creo o aggiorno il record in AAsset a partire da SourceAsseId corrente
            refAsset = Std_AAsset(Asset, AssetMatch, AssetRef)   
            # cerco le info sull'asset in Google        
            #gAsset = ParseGooglePlacesMain(Asset, AAsset)
            #if N_Ass > 100:
            #    break
        # chiudi DB
        t2 = time.clock()
        print(round(t2-t1, 3))
        return True

    except Exception as err:
        log(ERROR, err)
        return False

def Std_Asset(Curasset, Mode):

    if trace: log(DEBUG)   
    try:
        t1 = time.clock()
        tabratio = []        
        
        # record corrente
        curasset        = Curasset['Asset']
        curaasset       = Curasset['AAsset']
        curname         = Curasset['NameSimple'].lower()
        curcountry      = Curasset['Country'].lower()
        curweb          = Curasset['AddrWebsite'].lower()
        curphone        = Curasset['AddrPhone']
        curcity         = Curasset['AddrCity'].lower()
        curstreet       = Curasset['AddrStreet'].lower()
        curregion       = Curasset['AddrRegion'].lower()
        curzip          = Curasset['AddrZIP']
        curformatted    = Curasset['FormattedAddress'].lower()

        if Mode == "NEW":
            if Curasset['AAsset'] != 0:   # se e' gia'  stato battezzato non lo esamino di nuovo
                return curasset, curaasset
            # tutti i record dello stesso tipo e paese ma differenti source, e che hanno gia un asset di riferimento (aasset)
        cLite.execute("select * from MemAsset where Asset <> ? and Source <> ? and Country = ? and Assettype = ? and AAsset <> 0", (Curasset['Asset'], Curasset['Source'], Curasset['Country'], Curasset['AssetType']))
        rows  = cLite.fetchall()     
        if len(rows) == 0:   # non ce ne sono
            return 0,0   #inserisco l'asset corrente

        for j in range (0, len(rows)):

            name = cfrname = city = cfrcity = street = cfrstreet = zip = cfrzip = ''
            gblratio = 0; quanti = 0; 
            
            # record di confronto
            cfrasset        = rows[j][0]
            cfraasset       = rows[j][2]
            country         = str(rows[j][1])
            name            = str(rows[j][3].encode("utf-8"))
            source          = str(rows[j][4])
            cfrname         = str(rows[j][5].encode("utf-8")).decode("UTF8").lower()
            assettype       = str(rows[j][6])
            addrstreet      = str(rows[j][7].encode("utf-8"))
            addrcity        = str(rows[j][8].encode("utf-8"))
            addrzip         = str(rows[j][9])  
            addrcounty      = str(rows[j][10].encode("utf-8"))
            addrphone       = str(rows[j][11].encode("utf-8"))
            website         = str(rows[j][12].encode("utf-8"))
            addrregion      = str(rows[j][13].encode("utf-8"))
            formatted       = str(rows[j][14].encode("utf-8"))
            namesimplified  = str(rows[j][15])

            # per fuzzy confronto devo decodificare
            cfrcity         = addrcity.decode("utf8").lower()
            #cfrname         = namesimple.decode("utf8").lower()            
            cfrstreet       = addrstreet.decode("utf-8").lower()                                               
            cfrzip          = addrzip.decode("UTF-8").lower()
            cfrphone        = addrphone.decode("utf-8").lower()
            cfrweb          = website.decode("utf-8").lower()
            cfrregion       = addrregion.decode("utf8").lower()            
            cfrcountry      = country.decode("utf8").lower()            
            cfrformatted    = formatted.decode("utf8").lower()            

            # se hanno esattamente stesso sito web o telefono o indirizzo sono uguali
            if curweb: 
                if curweb == cfrweb:
                    return cfrasset, cfraasset
            if curphone: 
                if curphone == cfrphone:
                    return cfrasset, cfraasset
            # se c'e' almeno la strada e la citta', se l'indirizzo è uguale sono uguali
            if curcity and curstreet and curformatted:
                if curformatted == cfrformatted:
                    return cfrasset, cfraasset
            # se non hanno lo stesso paese, regione, provincia, salto
            if curcountry and cfrcountry and curcountry != cfrcountry:
                continue
            if curregion and curregion and curregion != cfrregion:
                continue

            nameratio=nameratio_ratio=nameratio_set=nameratio_partial=0           
            streetratio=streetratio_set=streetratio_partial=streetratio_ratio=0
            cityratio_ratio=cityratio_set=cityratio_partial=cityratio=0             
            webratio=phoneratio=zipratio=0
            # uso il nome standard
            nameratio_ratio = fuzz.ratio(curname, cfrname)
            nameratio_partial = fuzz.partial_ratio(curname, cfrname)
            nameratio_set = fuzz.token_set_ratio(curname, cfrname)
            nameratio = nameratio_set+ nameratio_partial + nameratio_ratio

            if nameratio_ratio > 79:                
                msg = "GT 80; %s; %s; %s;" % (curname, cfrname, round(nameratio_ratio,2))                
                log(WARNING, msg)                
                quanti = quanti + 1
            else:
                continue

            if curcity and cfrcity:                
                cityratio_ratio = fuzz.ratio(city, cfrcity)
                cityratio_partial = fuzz.partial_ratio(city, cfrcity)
                cityratio_set = fuzz.token_set_ratio(city, cfrcity)
                cityratio = cityratio_set + cityratio_partial + cityratio_ratio
                if cityratio > 79:
                    quanti = quanti + 1                
                else:
                    cityratio = 0

            if curstreet and cfrstreet:            
                streetratio_ratio = fuzz.ratio(curstreet, cfrstreet)
                streetratio_partial = fuzz.partial_ratio(street, cfrstreet)
                streetratio_set = fuzz.token_set_ratio(street, cfrstreet)
                streetratio = streetratio_set + streetratio_partial + streetratio_ratio
                if streetratio > 79:
                    quanti = quanti + 1 
                else:
                    streetratio = 0 
            if curweb and cfrweb:
                webratio = fuzz.ratio(curweb, cfrweb)
                if webratio > 79:
                    quanti = quanti + 1
                else:
                    webratio = 0

            if curphone and cfrphone:                 
                phoneratio = fuzz.ratio(curphone, cfrphone)
                if phoneratio > 79:
                    quanti = quanti + 1
                else:
                    phoneratio = 0

            if curzip and addrzip:
                zip = addrzip.title()
                zipratio = fuzz.ratio(curzip, cfrzip)
                if zipratio > 79:
                    quanti = quanti + 1
                else:
                    zipratio = 0
            
            check = (cityratio + streetratio + webratio + phoneratio) / quanti
            msg = ("CHECK %s" % (check))                
            log(WARNING, msg)                

            if check > 0:

                # peso i match 
                namepeso = 2
                streetpeso = 1.5
                citypeso = 1
                zippeso = 1
                webpeso = 1
                phonepeso = 1
                gblratio =( ((nameratio     * namepeso) +             \
                             (streetratio   * streetpeso) +           \
                             (cityratio     * citypeso) +             \
                             (zipratio      * zippeso) +              \
                             (webratio      * webpeso) +              \
                             (phoneratio    * phonepeso))             \
                             /
                             (quanti)  )                     
                tabratio.append((round(gblratio,2), curasset, curname, cfrname, cfrasset, cfraasset, round(nameratio,2), round(streetratio,2), round(cityratio,2), round(zipratio,2), round(webratio,2), round(phoneratio,2), round(nameratio_ratio,2), round(nameratio_partial,2), round(nameratio_set,2)))
            
        if len(tabratio) > 0:
            tabratio.sort(reverse=True, key=lambda tup: tup[0])
            if debug:
                Std_DumpTabratio(tabratio)
                cMsAcc.commit()
            if tabratio[0][0] > 400:   # global                
                msg = ("[ASSET MATCH] [%s-%s] [%s-%s] [%s]" % (tabratio[0][3], tabratio[0][1], tabratio[0][4], tabratio[0][2], tabratio[0][0]))
                log(WARNING, msg)
                #t2 = time.clock()
                #print(round(t2-t1, 3))
                return tabratio[0][3], tabratio[0][4]  # Asset, AAsset
        t2 = time.clock()
        print(round(t2-t1, 3))
        return 0,0

    except Exception as err:
        log(ERROR, err)
        return False

def Main():

    if genera:
        rc = Genera_ExtractName()
        if not rc:
            return False
    if nomi:
        rc = Names_Main()
        if not rc:
            return False
    if std:
        rc = Std_Main()
        if not rc:
            return False

    return True

def Main_CloseDb():
    # chiudi DB
    MsAcc.commit()
    MsAcc.close()
    MySql.commit()
    MySql.close()
    SqLite.close()

    return True

def Main_OpenDb():
    global cMsAcc, cLite, SqLite, MsAcc, xMySql, cMySql, MySql
    try:
        # apri connessione e cursori
        MsAcc  = pypyodbc.connect(Dsn)
        cMsAcc = MsAcc.cursor()
        if testrun:
            MySql  = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db='orange', use_unicode=True, charset='utf8')
        else:
            MySql  = pymysql.connect(host='54.77.219.201', port=3306, user='orange', passwd='5Q34jMBDy88ec8dc', db='orange', use_unicode=True, charset='utf8')
        MySql.autocommit(True)
        cMySql = MySql.cursor(pymysql.cursors.DictCursor)
        xMySql = MySql.cursor(pymysql.cursors.SSCursor)
        SqLite = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        cLite = SqLite.cursor()
        return True

    except Exception as err:
        log(ERROR, err)
        return False


if __name__ == "__main__":
    
    testrun = Dsn = debug = nomi = genera = trace = std = ''
    testrun, Dsn, debug, genera, nomi, trace, std = ParseArgs()

    rc = Main_OpenDb()
    if not rc:
        print("ERRORE, Open DB terminato in modo errato")        
        sys.exit(12)
    rc = Main()
    if not rc:
        log(ERROR, "Run terminato in modo errato")        
        sys.exit(12)
    else:
        log(INFO, "Run terminato in modo corretto")        
        
    rc = Main_CloseDb()



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

