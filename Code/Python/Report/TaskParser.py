from __future__ import print_function

import os.path

from datetime import datetime

from itertools import groupby
from operator import itemgetter

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/tasks.readonly']


def main():
    """Shows basic usage of the Tasks API.
    Prints the title and ID of the first 10 task lists.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('tasks', 'v1', credentials=creds)

        # Call the Tasks API
        optionalArgs = {'maxResults=100', 'showHidden=true', 'showDeleted=true'}
        results = service.tasklists().list(maxResults=100).execute()
        items = results.get('items', [])

        if not items:
            print('No task lists found.')
            return

        for item in items:
            tasklistTitle = item['title']
            filehandle = open('{0}.tex'.format(tasklistTitle), 'w')
            tasklistId = item['id']
            results = service.tasks().list(tasklist=tasklistId,
                                           showHidden=True,
                                           showCompleted=True).execute()
            items = results.get('items', [])
            tasks_prime = []
            for item in items:
                taskTitle   = item['title'].replace("...", "").replace("_", "\_").strip()
                taskDue = datetime.strptime(item['due'], '%Y-%m-%dT%H:%M:%S.%fZ').date() 
                tasks_prime.append([taskDue, tasklistTitle, taskTitle])
                print(u'{2}\t{0}\t{1}'.format(tasklistTitle, taskTitle, taskDue))
            print("")

            tasks_prime.sort(key = itemgetter(0))
            tasks = []
            dates = []
            for date, task in groupby(tasks_prime, itemgetter(0)):
                tasks.append(list(task))
                dates.append(date)
            for (date, tasks) in zip(dates, tasks):
                line = ", ".join([task[2] for task in tasks])
                latex = "{0} & 4 & {1}. \\\\[10pt] \hline".format(date, line)
                filehandle.write("{0}\n".format(latex))
                print(latex)
            print("\n")
            filehandle.close()            
                
    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()
