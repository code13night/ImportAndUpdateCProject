import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Date, Time, INTEGER, VARCHAR 
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
#connection to DB
db_string = "postgres://postgres:qwe123asd@localhost:5432/TEST"
db = create_engine(db_string)  
base = declarative_base()

class MaterialTable(base):  
    __tablename__ = 'Material'

    ID = Column(INTEGER, primary_key=True)
    material = Column(VARCHAR)

class ProductionOrderTable(base):  
    __tablename__ = 'ProductionOrder'

    ID = Column(INTEGER, primary_key=True)
    productionOrder = Column(VARCHAR)

class SiteTable(base):  
    __tablename__ = 'Site'

    ID = Column(INTEGER, primary_key=True)
    site = Column(VARCHAR)
    siteName = Column(VARCHAR)

class SupervisorCodeTable(base):  
    __tablename__ = 'SupervisorCode'

    ID = Column(INTEGER, primary_key=True)
    supervisorCode = Column(VARCHAR)

class WorkCentreTable(base):  
    __tablename__ = 'WorkCentre'

    ID = Column(INTEGER, primary_key=True)
    workCentre = Column(VARCHAR)

class ActivityTable(base):  
    __tablename__ = 'Activity'

    ID = Column(INTEGER, primary_key=True)
    activity = Column(VARCHAR)


class ProductOrderTransactionTable(base):  
    __tablename__ = 'ProductionOrderTransaction'

    ID = Column(INTEGER, primary_key=True)
    productionOrderID = Column(INTEGER)
    materialID = Column(INTEGER)
    siteID = Column(INTEGER)
    supervisorCodeID = Column(INTEGER)
    workCentreID = Column(INTEGER)
    activityID = Column(INTEGER)
    totalOrderQuantity = Column(INTEGER)
    confirmedScrapQuantity = Column(INTEGER)
    confirmedGoodQuantity = Column(INTEGER)
    productionOrderStatus = Column(INTEGER)
    actualProductionReleaseDate = Column(Date)
    basicStartDate = Column(Date)
    basicStartTime = Column(Time)
    basicFinishDate = Column(Date)
    basicFinishTime = Column(Time)
    productionOrderStatus= Column(VARCHAR)
    actualStartDate = Column(Date)
    actualStartTime = Column(Time)
    actualFinishDate = Column(Date)
    actualFinishTime = Column(Time)
    scheduledStartDate = Column(Date)
    scheduledStartTime = Column(Time)
    scheduledFinishDate = Column(Date)
    scheduledFinishTime = Column(Time)


Session = sessionmaker(db)  
session = Session()

#read excel
df = pd.read_excel('ImportDataForProductionOrderTransaction.xlsx', sheet_name='Sheet1')
startcount=0
for i in df.index:
	startcount=startcount+1
	#getIDform productionOrder
	productionOrderVal=df['productionOrder'][i]
	productionOrderID = session.query(ProductionOrderTable).filter(ProductionOrderTable.productionOrder==str(productionOrderVal)).first()

	#getIDform material
	materialVal=df['material'][i]
	materialID = session.query(MaterialTable).filter(MaterialTable.material==str(materialVal)).first()

	#getIDform supervisorCode
	supervisorCodeVal=df['supervisorCode'][i]
	supervisorCodeID = session.query(SupervisorCodeTable).filter(SupervisorCodeTable.supervisorCode==str(supervisorCodeVal)).first()

	#getIDform site
	siteVal=df['site'][i]
	siteID = session.query(SiteTable).filter(SiteTable.site==str(siteVal)).first()

	totalOrderQuantityVal=int(df['totalOrderQuantity'][i])
	basicStartDateVal=str(df['basicStartDate'][i])
	if basicStartDateVal == 'NaT':
		basicStartDateVal=None
	basicStartTimeVal=str(df['basicStartTime'][i])
	if basicStartTimeVal == 'NaT':
		basicStartTimeVal=None
	basicFinishDateVal=str(df['basicFinishDate'][i])
	if basicFinishDateVal =='NaT':
		basicFinishDateVal=None
	basicFinishTimeVal=str(df['basicFinishTime'][i])
	if basicFinishTimeVal =='NaT':
		basicFinishTimeVal=None
	productionOrderStatusVal=df['productionOrderStatus'][i]
	actualStartDateVal=str(df['actualStartDate'][i])
	if actualStartDateVal =='NaT':
		actualStartDateVal=None
	actualStartTimeVal=str(df['actualStartTime'][i])
	if actualStartTimeVal =='NaT':
		actualStartTimeVal=None
	actualFinishDateVal=str(df['actualFinishDate'][i])
	if actualFinishDateVal =='NaT':
		actualFinishDateVal=None
	actualFinishTimeVal=str(df['actualFinishTime'][i])
	if actualFinishTimeVal =='NaT':
		actualFinishTimeVal=None
	actualProductionReleaseDateVal=str(df['actualProductionReleaseDate'][i])
	if actualProductionReleaseDateVal =='NaT':
		actualProductionReleaseDateVal=None
	confirmedScrapQuantityVal=int(df['confirmedScrapQuantity'][i])
	scheduledStartDateVal=str(df['scheduledStartDate'][i])
	if scheduledStartDateVal =='NaT':
		scheduledStartDateVal=None
	scheduledStartTimeVal=str(df['scheduledStartTime'][i])
	if scheduledStartTimeVal =='NaT':
		scheduledStartTimeVal=None
	scheduledFinishDateVal=str(df['scheduledFinishDate'][i])
	if scheduledFinishDateVal =='NaT':
		scheduledFinishDateVal=None
	scheduledFinishTimeVal=str(df['scheduledFinishTime'][i])
	if scheduledFinishTimeVal =='NaT':
		scheduledFinishTimeVal=None
	confirmedGoodQuantityVal=int(df['confirmedGoodQuantity'][i])

	string_to_print=str(productionOrderVal)+".... done !!!"

	if productionOrderID:
		productionOrderIDVal=int(productionOrderID.ID)
	else:
		productionOrderIDVal=None
		string_to_print=str(productionOrderVal)+".... productionOrder Not Found !!!"
	if materialID:
		materialIDVal=int(materialID.ID)
	else:
		materialIDVal=None
		string_to_print=str(productionOrderVal)+".... material Not Found !!!"
	if supervisorCodeID:
		supervisorCodeIDVal=int(supervisorCodeID.ID)
	else:
		materialIDVal=None
		string_to_print=str(productionOrderVal)+".... supervisorCode Not Found !!!"
	if siteID:
		siteIDVal=int(siteID.ID)
	else:
		siteIDVal=None
		string_to_print=str(productionOrderVal)+".... site Not Found !!!"

	insertValue=ProductOrderTransactionTable(ID=startcount, 
		productionOrderID = productionOrderIDVal,
		materialID = materialIDVal,
		siteID = siteIDVal,
		supervisorCodeID = supervisorCodeIDVal,
		totalOrderQuantity = totalOrderQuantityVal,
		confirmedScrapQuantity = confirmedScrapQuantityVal,
		confirmedGoodQuantity = confirmedGoodQuantityVal,
		productionOrderStatus = productionOrderStatusVal,
		actualProductionReleaseDate = actualProductionReleaseDateVal,
		basicStartDate = basicStartDateVal,
		basicStartTime = basicStartTimeVal,
		basicFinishDate = basicFinishDateVal,
		basicFinishTime = basicFinishTimeVal,
		actualStartDate = actualStartDateVal,
		actualStartTime = actualStartTimeVal,
		actualFinishDate = actualFinishDateVal,
		actualFinishTime = actualFinishTimeVal,
		scheduledStartDate = scheduledStartDateVal,
		scheduledStartTime = scheduledStartTimeVal,
		scheduledFinishDate = scheduledFinishDateVal,
		scheduledFinishTime = scheduledFinishTimeVal
		)
	session.add(insertValue)  
	session.commit()

	
	print(string_to_print)

print("done")

	


