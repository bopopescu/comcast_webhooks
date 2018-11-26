import boto3
import mysql.connector
import urllib.parse
import json
import uuid
import re



def getFAQ(phone_name,faq_name,database,level):
	

	db = mysql.connector.connect(user='botintent_usr', password='Lqh28hWIU92UiFf',host='tracphone-devicebits-secure-cluster.cluster-coswbbhdjjqu.us-east-1.rds.amazonaws.com',database=database)
#	quickReplies = []
	select = ""
	args = None
	faq_flag = False
	

	select =  "select faq.name,faq.pk,val,phonefk from faq join phone on phone.pk = faq.phonefk where phone.name = %s and (faq.name = concat(%s,' Agent Instructions') or faq.name = %s) order by faq.pk desc"
	args = phone_name,faq_name,faq_name
	cur = db.cursor(dictionary=True)
	cur.execute(select,args)
	result_val = cur.fetchall()
	reply_val = {}
	reply_val["quickReplies"] = []
	reply_val["quickReplies"].append({"text":"Problem Resolved"})
	for row in result_val:
	    if "AGENT INSTRUCTIONS" in row ["name"].upper():
		    if "continueButton" in row["val"]:
		    	instructionList = row["val"].split("<continueButton>")
		    elif "nextButton" in row["val"]:
		    	instructionList = row["val"].split("<nextButton>")
		    else:
		    	instructionList = []
		    	instructionList.append(row["val"])
		    reply_val["faqNext"] = len(instructionList)
		    reply_val["faqFirst"] = instructionList[0]
		    level = int(level)
		    if level > len(instructionList):
		    	reply_val["quickReplies"] = []
		    	if "continueButton" in row["val"]:
		    		reply_val["responseOverride"] = "That is all of the support that I have for this issue. If you want more assistance, you must escalate to your supervisor."
		    		reply_val["quickReplies"].append({"text":"Restart"})
		    		reply_val["levelVal"] = 1
		    		continue
		    elif "nextButton" in row["val"] and level >= len(instructionList):
		    	reply_val["quickReplies"] = []
		    	reply_val["responseOverride"] = instructionList[level-1]
		    	reply_val["quickReplies"].append({"text":"Problem Resolved"})
		    	reply_val["levelVal"] = 1	
		    	continue
		    else:
		    	reply_val["responseOverride"] = instructionList[level-1]
		    	reply_val["levelVal"] = level + 1
		    	#i3f level >len(instructionList)-1:
		    	
		    	
		    	if "continueButton" in row["val"]:
		    		reply_val["quickReplies"].append({"text":"Problem Not Resolved"})
		    		if level > len(instructionList):
		    			reply_val["quickReplies"] = []
		    			reply_val["responseOverride"] = "That is all of the support that I have for this issue. If you want more assistance, you must escalate to your supervisor."
		    			reply_val["quickReplies"].append({"text":"Restart"})
		    			reply_val["levelVal"] = 1	
		    	elif "nextButton" in row["val"]:
		    		reply_val["quickReplies"] = []
		    		reply_val["quickReplies"].append({"text":"Next Step"})
		    		if level > len(instructionList):
		    			reply_val["quickReplies"] = []
		    			reply_val["quickReplies"].append({"text":"Problem Resolved"})
		    			reply_val["levelVal"] = 1	
		    	if "continueButton" not in row["val"] and "nextButton" not in row["val"]:
		    		reply_val["levelVal"] = 1
			
	    if "AGENT INSTRUCTIONS" not in row['name'].upper():
		  #   reply_val["quickReplies"].append({"type":"event","name":"dbNav","value" :"http://trc.devicebits.com/expresshelp/error-codes/" + str(row["pk"]),"text":"More Error Info"})
		     reply_val["currentFAQ"] = row['name']
		     reply_val["dbNav"] = "/faq/" + str(row["pk"]) + "?device=" + str(row["phonefk"]) #25438
	print(cur._executed)
	cur.close()
	db.close()
#	for row in result_val:
#		quickReplies.append(row['name'])
	cur.close()
	db.close()

	return reply_val


def lambda_handler(event, context):
#    event["body"] = 'from=6807035d-1703-4011-9b17-e06c466d3b6f&to=48299&reply=My phone is having sound problems&botID=48299&moduleID=1343420&inResponseTo=1343621&moduleNickname=JCM TEst Lambda&replyData=&nlpData={"source":"this is why I hate lambda","intents":[],"act":"wh-query","type":"desc:reason","sentiment":"vnegative","entities":{"pronoun":[{"person":1,"number":"singular","gender":"unknown","raw":"I","confidence":0.99}]},"language":"en","processing_language":"en","timestamp":"2018-02-07T18:33:28.650871+00:00","status":200}&replyHistory=[{"id":1343621,"extractedData":null,"nlpData":{"source":"this is why I hate lambda","intents":[],"act":"wh-query","type":"desc:reason","sentiment":"vnegative","entities":{"pronoun":[{"person":1,"number":"singular","gender":"unknown","raw":"I","confidence":0.99}]},"language":"en","processing_language":"en","timestamp":"2018-02-07T18:33:28.650871+00:00","status":200},"raw":"this%20is%20why%20I%20hate%20lambda"}]&attachedMedia={"types":[],"urls":[],"coords":[]}&direction=in&secret=hello_motion&webhookType=module&session=6807035d-1703-4011-9b17-e06c466d3b6f&updatedAt=2018-02-07T18:33:28.834Z'

    #faq_fk = '1656277'
    #customer ='CreditOneBank'
    #database_ver = "tutorialMasterV6"
    faq_name = "startModule"
    phone='trc-comcast-bot'
    #&faqName=Missing SVC for Eq Type&databaseVer=tutorialMasterV6
    faqName='Missing SVC for Eq Type'
    faq_name = urllib.parse.unquote(event["queryStringParameters"]["faqName"])
    phone =  urllib.parse.unquote(event["queryStringParameters"]["phone"])
    level =  urllib.parse.unquote(event["queryStringParameters"]["level"])
    currentFAQ = urllib.parse.unquote(event["queryStringParameters"]["currentFAQ"])
    #database_ver =  urllib.parse.unquote(event["queryStringParameters"]["databaseVer"])
    database_ver = 'dbdevelopment'
    resp = {}
    if faq_name.upper().strip() == "PROBLEM NOT RESOLVED" or faq_name.upper().strip() == "NEXT STEP":
    	faq_name = currentFAQ 
    	resp = getFAQ(phone,faq_name,database_ver,level)
    elif faq_name.upper() == "XFINITY VIDEO":
    	resp["responseOverride"] = "Choose one of the error messages below:"
    	resp["quickReplies"] = [{"text": "CEM19E"},{"text": "ISP17E"}]
    elif faq_name.upper() == "XFINITY VOICE":
    	resp["responseOverride"] = "Choose one of the error messages below:"
    	resp["quickReplies"] =  [{"text": "I2E20E"}]
    elif faq_name.upper() == "XFINITY INTERNET":
    	resp["responseOverride"] = "Choose one of the error messages below:"
    	resp["quickReplies"] =  [{ "text": "Missing SVC for Eq Type"},{"text": "ISP17E"}]
    elif faq_name == "3365" or faq_name.upper() == "ERR3365":
    	resp = getFAQ(phone,"Missing SVC for Eq Type",database_ver,level)
    elif faq_name == "1750" or faq_name.upper() == "ERR1750":
    	resp = getFAQ(phone,"ISP17E",database_ver,level)
    elif faq_name == "2569" or faq_name.upper() == "ERR2569":
    	resp = getFAQ(phone,"CEM19E",database_ver,level)
    elif faq_name == "1968" or faq_name.upper() == "ERR1968":
    	resp = getFAQ(phone,"I2E20E",database_ver,level)
    elif faq_name == "1968" or faq_name.upper() == "RDK ISSUES":
    	resp["responseOverride"] = "Please select an error below"
    	resp["quickReplies"] =  [{"text": "RDK-03060"}]
    else:
    	resp = getFAQ(phone,faq_name,database_ver,level)
    output_new = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": { "content-type": "application/json","Access-Control-Allow-Origin" : "*"},
        "body":json.dumps(resp)
    }
    return output_new