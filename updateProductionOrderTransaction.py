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

class ProductionOrderTable(base):  
    __tablename__ = 'ProductionOrder'

    ID = Column(INTEGER, primary_key=True)
    productionOrder = Column(VARCHAR)

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
    lastMoveDate = Column(Date)


Session = sessionmaker(db)  
session = Session()

#read excel
df = pd.read_excel('UpdateDataForProductionOrderTransaction.xlsx', sheet_name='Sheet1')
startcount=0
for i in df.index:
	startcount=startcount+1
	#getIDform productionOrder
	productionOrderVal=df['productionOrder'][i]
	productionOrderID = session.query(ProductionOrderTable).filter(ProductionOrderTable.productionOrder==str(productionOrderVal)).first()
	if productionOrderID:
		productionOrderIDVal=int(productionOrderID.ID)
	else:
		string_to_print=str(productionOrderVal)+".... productionOrderID Not Found !!!"
		productionOrderIDVal=None

	string_to_print=str(productionOrderVal)+".... done !!!"

	#getIDform workCentre
	workCentreVal=df['workCentre'][i]
	if workCentreVal=='NaT':
		workCentreIDVal=None
	else:
		print(str(workCentreVal)[:-3])
		workCentreID=session.query(WorkCentreTable).filter(WorkCentreTable.workCentre==str(workCentreVal)[:-3]).first()
		if workCentreID:
			workCentreIDVal=int(workCentreID.ID)
		else:
			string_to_print=str(productionOrderVal)+".... workCentreID Not Found !!!"
			workCentreIDVal=None


	#getIDform Activity
	activityVal=df['activity'].apply(lambda x: '{0:0>4}'.format(x))[i]
	if activityVal=='NaT':
		activityIDVal=None
	else:
		print(activityVal)
		activityID=session.query(ActivityTable).filter(ActivityTable.activity==str(activityVal)).first()
		if activityID:
			activityIDVal=int(activityID.ID)
		else:
			string_to_print=str(productionOrderVal)+".... activityID Not Found !!!"
			activityIDVal=None


	lastMoveDateVal=str(df['lastMoveDate'][i])
	if lastMoveDateVal =='NaT':
		lastMoveDateVal=None

	
	if productionOrderID:
		updatedRow=session.query(ProductOrderTransactionTable).filter(ProductOrderTransactionTable.ID==productionOrderIDVal).first()
		updatedRow.workCentreID=workCentreIDVal
		updatedRow.activityID=activityIDVal
		updatedRow.lastMoveDate=lastMoveDateVal
		session.commit()

	
	print(string_to_print)

print("done")

	


