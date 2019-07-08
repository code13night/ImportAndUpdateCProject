import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Date, Time, INTEGER, VARCHAR
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
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

class SiteTable(base):  
    __tablename__ = 'Site'

    ID = Column(INTEGER, primary_key=True)
    site = Column(VARCHAR)
    siteName = Column(VARCHAR)

class WorkCentreTable(base):  
    __tablename__ = 'WorkCentre'

    ID = Column(INTEGER, primary_key=True)
    workCentre = Column(VARCHAR)

class ActivityTable(base):  
    __tablename__ = 'Activity'

    ID = Column(INTEGER, primary_key=True)
    activity = Column(VARCHAR)


class ProductionActivityTransactionTable(base):  
    __tablename__ = 'ProductionActivityTransaction'

    ID = Column(INTEGER, primary_key=True)
    productionOrderID = Column(INTEGER)
    siteID = Column(INTEGER)
    workCentreID = Column(INTEGER)
    activityID = Column(INTEGER)
    totalOrderQuantity = Column(INTEGER)
    confirmedYield = Column(INTEGER)
    activityOrderStatus = Column(VARCHAR)
    actualStartDateExecution = Column(Date)
    actualStartTimeExecution = Column(Time)
    actualFinishDateExecution = Column(Date)
    actualFinishTimeExecution = Column(Time)
    standardQueueTime = Column(DOUBLE_PRECISION)
    standardSetupTime = Column(DOUBLE_PRECISION)
    standardLabourTime = Column(DOUBLE_PRECISION)
    standardProcessTime = Column(DOUBLE_PRECISION)
    confirmedActivityScrapQuantity = Column(INTEGER)


Session = sessionmaker(db)  
session = Session()

#read excel
df = pd.read_excel('2800_Filter.xlsx', sheet_name='Sheet1')
startcount=33015
line=0
for i in df.index:
	line=line+1
	#getIDform productionOrder
	productionOrderVal=df['productionOrder'][i]
	productionOrderID = session.query(ProductionOrderTable).filter(ProductionOrderTable.productionOrder==str(productionOrderVal)).first()


	if productionOrderID:
		startcount=startcount+1
		productionOrderIDVal=int(productionOrderID.ID)

		#getIDform site
		siteVal=df['site'][i]
		siteID = session.query(SiteTable).filter(SiteTable.site==str(siteVal)).first()

		#getIDform workCentre
		workCentreVal=df['workCentre'][i]
		if workCentreVal=='NaT':
			workCentreIDVal=None
		else:
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
			activityID=session.query(ActivityTable).filter(ActivityTable.activity==str(activityVal)).first()
			if activityID:
				activityIDVal=int(activityID.ID)
			else:
				string_to_print=str(productionOrderVal)+".... activityID Not Found !!!"
				activityIDVal=None

		totalOrderQuantityVal=int(df['totalOrderQuantity'][i])
		confirmedYieldVal=int(df['confirmedYield'][i])
		confirmedActivityScrapQuantityVal=int(df['confirmedActivityScrapQuantity'][i])
		activityOrderStatusVal=str(df['activityOrderStatus'][i])

		actualStartDateExecutionVal=str(df['actualStartExecutionDate'][i])
		if actualStartDateExecutionVal == 'NaT':
			actualStartDateExecutionVal=None

		actualStartTimeExecutionVal=str(df['actualStartExecutionTime'][i])
		if actualStartTimeExecutionVal == 'NaT':
			actualStartTimeExecutionVal=None

		actualFinishDateExecutionVal=str(df['actualFinishExecutionDate'][i])
		if actualFinishDateExecutionVal == 'NaT':
			actualFinishDateExecutionVal=None

		actualFinishTimeExecutionVal=str(df['actualFinishExecutionTime'][i])
		if actualFinishTimeExecutionVal== 'NaT':
			actualFinishTimeExecutionVal=None

		standardQueueTimeVal=float(df['standardQueueTime'][i])
		standardSetupTimeVal=float(df['standardSetupTime'][i])
		standardLabourTimeVal=float(df['standardLabourTime'][i])
		standardProcessTimeVal=float(df['standardProcessTime'][i])

		string_to_print=str(productionOrderVal)+".... done !!!"
		if siteID:
			siteIDVal=int(siteID.ID)
		else:
			siteIDVal=None
			string_to_print=str(productionOrderVal)+".... site Not Found !!!"

		insertValue=ProductionActivityTransactionTable(ID=startcount, 
			productionOrderID = productionOrderIDVal,
			siteID = siteIDVal,
			workCentreID= workCentreIDVal,
			activityID=activityIDVal,
			totalOrderQuantity = totalOrderQuantityVal,
			confirmedYield=confirmedYieldVal,
			activityOrderStatus=activityOrderStatusVal,
			actualStartDateExecution = actualStartDateExecutionVal,
			actualStartTimeExecution = actualStartTimeExecutionVal,
			actualFinishDateExecution = actualFinishDateExecutionVal,
			actualFinishTimeExecution = actualFinishTimeExecutionVal,
			standardQueueTime = standardQueueTimeVal,
			standardSetupTime = standardSetupTimeVal,
			standardLabourTime = standardLabourTimeVal,
			standardProcessTime = standardProcessTimeVal,
			confirmedActivityScrapQuantity=confirmedActivityScrapQuantityVal,
			)

		session.add(insertValue)  
		session.commit()
	else:
		productionOrderIDVal=None
		string_to_print=str(productionOrderVal)+".... productionOrder Not Found !!!"
	

	
	print(str(line)+" - "+string_to_print)

print("done")

	


