import schedule, os, time, logging 
import praw, psycopg2
from simple_salesforce import Salesforce
from slackclient import SlackClient

# Set Logging Level
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create SalesForce Login Object
sf = Salesforce(
    username=os.environ["sf_username"],
    password=os.environ["sf_password"],
    security_token=os.environ["SECURITY_TOKEN"]
)

# Establish DB Connection
DATABASE_URL = os.environ["FEDBOT_DB"]
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
cur = conn.cursor()

def sendMessage(slack_client, msg):
    # make the POST request through the python slack client
    updateMsg = slack_client.api_call(
        "chat.postMessage",
        channel='#fed-support',
        text=msg
    )

    # check if the request was a success
    if updateMsg['ok'] is not True:
        logging.error(updateMsg)
    else:
        logging.debug(updateMsg)


def sendBlock(slack_client, msg):
    slack_client.api_call("chat.postMessage",
                          channel="#fed-support",
                          text='Alert',
                          blocks=[
                              {
                                  "type": "section",
                                  "text": {
                                      "type": "mrkdwn",
                                      "text": f'{msg}'
                                  }
                              }
                          ])


def getQueueDetails():
    # Build Queue Details
    global QueueDetails

    # Query for cases in FED_WS1_ATL_POD queue
    q = sf.query(
        "SELECT CaseNumber, Contact.Name, Account.Name, First_Response_Due_In_in_minutes__c,Entitlement.Name, Subject, "
        "toLabel(Status), toLabel(Sub_Status__c), toLabel(Priority), CreatedDate, Commit_Time_Text__c, "
        "toLabel(Airwatch_Group__c), toLabel(GSS_Problem_Category__c), Id, RecordTypeId, CurrencyIsoCode, "
        "SystemModstamp, Contact.Id, ContactId, Contact.RecordTypeId, Account.Id, AccountId, Account.RecordTypeId, "
        "Entitlement.Id, EntitlementId, GSS_First_Resp_Met__c, GSS_Case__c, GSS_Case_Idle_Time_Days__c, EP_Bug_URL__c,"
        "Name_of_Entitlement__c, LastModifiedDate, EA_Name__c, Description "
        "FROM Case "
        "WHERE OwnerId = '00Gf4000002djYA' "
        "ORDER BY CaseNumber ASC NULLS FIRST, Id ASC NULLS FIRST"
    )

    records = q.get('records')
    for record in records:
        record_num = records.index(record)
        CaseNumber = record.get('CaseNumber')
        CaseLink = record.get('GSS_Case__c').split('"')[1]
        FirstResponseDue_minutes = record.get('First_Response_Due_In_in_minutes__c')
        FirstResponseMet = record.get('GSS_First_Resp_Met__c')
        Priority = record.get('Priority')
        Name_of_Entitlement__c = record.get('Name_of_Entitlement__c')
        Status = record.get('Status')
        Case_Idle_Time_Business_Days__c = record.get('Case_Idle_Time_Business_Days__c')
        EP_Bug_URL__c = record.get('EP_Bug_URL__c')
        EA_Name__c = record.get('EA_Name__c')
        Description = record.get('Description')

        # Build Case Details and add to Queue Details
        CaseDetails = {
            'CaseNumber': CaseNumber,
            'CaseLink': CaseLink,
            'FirstResponseDue_minutes': FirstResponseDue_minutes,
            'FirstResponseMet': FirstResponseMet,
            'Priority': Priority,
            'Status': Status,
            'EP_Bug_URL__c': EP_Bug_URL__c,
            'Case_Idle_Time_Business_Days__c': Case_Idle_Time_Business_Days__c,
            'Name_of_Entitlement__c': Name_of_Entitlement__c,
            'EA_Name__c': EA_Name__c,
            'Description': Description
        }

        QueueDetails['Case_' + str(record_num + 1)] = CaseDetails


def check_Priority():
    # Get Tickets in Federal Queue
    for case in QueueDetails.values():
        # CaseNumber = int(case.get('CaseNumber'))
        CaseNumber = case.get('CaseNumber')
        Priority = case.get('Priority')
        CaseLink = 'https://vmware-gs.lightning.force.com' + case.get('CaseLink')
        EA_Name__c = case.get('EA_Name__c')
        Description = case.get('Description')[0:200]

        logging.debug(f"CaseNumber is {CaseNumber}. Priority is: {Priority}")

        # Query DB for Ticket. Generate response list.
        cur.execute("SELECT priority FROM alreadyNotified")
        responses = [i[0] for i in cur.fetchall()]

        # Check case priority and if it exists in DB. If not, alert and add.
        if not CaseNumber in responses and (Priority.startswith("1") or Priority.startswith("2")):

            print(f"{CaseNumber} IS NOT IN {responses}. Updating DB...")
            
            # Add
            cur.execute(f"INSERT INTO alreadyNotified (priority) VALUES ({CaseNumber})")
            conn.commit()

            # Alert
            msg = f"<!here> {Priority} ALERT: <{CaseLink}|{CaseNumber}> has been added to the FED-WS1-ATL-POD queue.\n\n" \
                  f"Customer: {EA_Name__c}\n" \
                  f"Description: {Description}" 

            sendBlock(slack_client, msg)
        
        else:
            logging.debug(f"{CaseNumber} already added to TABLE alreadyNotified COLUMN Priority")


def check_CommitTime():
    for case in QueueDetails.values():

        CaseNumber = case.get('CaseNumber')
        CaseLink = 'https://vmware-gs.lightning.force.com' + case.get('CaseLink')
        FirstResponseDue_minutes = case.get('FirstResponseDue_minutes')
        FirstResponseMet = case.get('FirstResponseMet')

        logging.debug(f"CaseNumber: {CaseNumber} // FirstResponseDue_minutes: {FirstResponseDue_minutes} // FirstResponseMet: {FirstResponseMet}")


        # Query DB for alreadyNotified
        cur.execute("SELECT commit FROM alreadyNotified")
        responses = [i[0] for i in cur.fetchall()]

        if FirstResponseDue_minutes / 60 < 1 \
                and FirstResponseDue_minutes > 0 \
                and not CaseNumber in responses \
                and FirstResponseMet == None:

                logging.debug(f"{CaseNumber} not in {responses}. Updating DB...")

                # Add
                cur.execute(f"INSERT INTO alreadyNotified (commit) VALUES ({CaseNumber})")
                conn.commit()

                # Alert
                msg = f"<!here> <{CaseLink}|{CaseNumber}> has {FirstResponseDue_minutes} minutes until missed commit."
                logging.debug(f"{CaseNumber} Posted to Slack")
                sendBlock(slack_client, msg)

        else:
            logging.debug(f"{CaseNumber} already added to TABLE alreadyNotified COLUMN Commit")


def check_Entitlement():
    for case in QueueDetails.values():

        CaseNumber = case.get('CaseNumber')
        CaseLink = 'https://vmware-gs.lightning.force.com' + case.get('CaseLink')
        Name_of_Entitlement__c = case.get('Name_of_Entitlement__c').lower()
        EA_Name__c = case.get('EA_Name__c')


        # Query DB for alreadyNotified
        cur.execute("SELECT entitlement FROM alreadyNotified")
        responses = [i[0] for i in cur.fetchall()]

    if ('federal' not in Name_of_Entitlement__c) and (not CaseNumber in responses):

        logging.debug(f"{CaseNumber} not in {responses}. Updating DB...")

        # Add
        cur.execute(f"INSERT INTO alreadyNotified (entitlement) VALUES ({CaseNumber})")
        conn.commit()

        # Alert
        msg = f"<!here> <{CaseLink}|{CaseNumber}> does not have a Federal entitlement according to SalesForce.\n\n" \
              f"Customer: {EA_Name__c}\n" \
              f"Entitlement Type: {Name_of_Entitlement__c}"
        logging.debug("Posted to Slack")
        sendBlock(slack_client, msg)

    else:
        logging.debug(f"{CaseNumber} already added to TABLE alreadyNotified COLUMN Entitlement")


# def check_ProblemCategory():
#     q = sf.query(
#         "SELECT CaseNumber,Case_Owner_Name__c,GSS_Problem_Category__c, GSS_Case__c "
#         "FROM Case "
#         "WHERE Case_Owner_Name__c IN "
#         "('Ryan Prisco', 'Gia Cao', 'Adam Evancho', 'Mark Curbeam', 'Nick Moyer', Travis Williams, Steven Marcolla) "
#         "AND Status != 'Closed'"
#     )

#     records = q.get('records')
#     Prob_Cat_Violators = {}
#     for record in records:
#         CaseNumber = record.get('CaseNumber')
#         WS1_Prob_Cat = record.get('GSS_Problem_Category__c')
#         CaseOwner = record.get('Case_Owner_Name__c')
#         CaseLink = record.get('GSS_Case__c').split('"')[1]
#         if WS1_Prob_Cat == 'Workspace One':
#             if CaseOwner not in Prob_Cat_Violators.keys():
#                 Prob_Cat_Violators[f'{CaseOwner}'] = [CaseNumber, CaseLink]
#             else:
#                 Prob_Cat_Violators[f'{CaseOwner}'].append(CaseNumber)
#                 Prob_Cat_Violators[f'{CaseOwner}'].append(CaseLink)

#     msg = f'Halftime Report:\nPlease see the following tickets with Problem Category of Workspace One:\n\n'
#     for k, v in Prob_Cat_Violators.items():
#         msg += k + ': \n'
#         for i in range(0, len(v) - 1, 2):
#             msg += '- ' + v[i] + ' ' + 'https://vmware-gs.lightning.force.com' + v[i + 1] + '\n'
#     sendMessage(slack_client, msg)


def check_IdleTime():
    query = sf.query("""SELECT CaseNumber,Case_Owner_Name__c,Case_Idle_Time_Business_Days__c,Id
                        FROM Case
                        WHERE Case_Owner_Name__c in ('Ryan Prisco', 'Gia Cao', 'Adam Evancho', 
                        'Mark Curbeam', 'Steven Marcolla','Travis Williams', 'Nick Moyer')
                        AND Case_Idle_Time_Business_Days__c > 2
                        AND Status != 'Closed'
                        AND EP_Bug_URL__c = null""")['records']

    msg = f"*Halftime Report:*\n\n" \
          f"Tickets with 2+ Day Idle Times:\n"
    for ticket in query:
        msg += f"{ticket['Case_Owner_Name__c']} " \
               f"- Idle Time: {ticket['Case_Idle_Time_Business_Days__c']} " \
               f"- Ticket: <https://vmware-gs.lightning.force.com/lightning/r/Case/" + f"{ticket['Id']}" + '/view'\
               f"|{ticket['CaseNumber']}>\n"

    sendBlock(slack_client, msg)


# def getQuote():
#     global quote_of_the_day
#     r = praw.Reddit(client_id=os.environ['reddit_client_id'],
#                     client_secret=os.environ['reddit_client_secret'],
#                     user_agent=os.environ['reddit_user_agent'])

#     url_rising = r.subreddit('QuotesPorn').random_rising().url
#     url_hot = r.subreddit('QuotesPorn').hot().url
#     url_new = r.subreddit('QuotesPorn').new().url
#     params = {"limit": 1}
#     posts = r.request('GET', url_new, params=params)

#     # Filter data
#     children = posts.get('data').get('children')
#     for i in children:
#         for n in i.get('data').get('preview').get('images'):
#             quote_of_the_day = n.get('source').get('url')

#     msg = f'Good Morning Federal Agents!\n\nToday\'s Quote of the Day provided by:\n {quote_of_the_day}'
#     sendMessage(slack_client, msg)


# Run Jobs
if __name__ == "__main__":
    SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN_FEDBOT"]
    slack_client = SlackClient(SLACK_BOT_TOKEN)
    logging.debug("authorized slack client")

    QueueDetails = {}
    alreadyNotified_Entitlement = []
    alreadyNotified_CommitTime = []
    alreadyNotified_Priority = []
    schedule.every(30).seconds.do(getQueueDetails)
    schedule.every(30).seconds.do(check_Priority)
    schedule.every(30).seconds.do(check_CommitTime)
    schedule.every(30).seconds.do(check_Entitlement)
    # schedule.every(1).day.at('12:00').do(getQuote)
    schedule.every(1).day.at('17:00').do(check_IdleTime)
    # schedule.every(1).day.at('17:00').do(check_ProblemCategory)

    while True:
        schedule.run_pending()
        time.sleep(5)  # sleep for 5 seconds between checks on the scheduler
