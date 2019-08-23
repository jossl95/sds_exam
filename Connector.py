### Define our Connector

import requests,os,time
def ratelimit():
    "A function that handles the rate of your calls."
    time.sleep(0.5) # sleep one second.

class Connector():
    def __init__(self, logfile, overwrite_log=False ,connector_type='requests'\
              ,session=False ,path2selenium='' ,n_tries = 5 ,timeout=30 ):
    """
    This Class implements a method for reliable connection to the internet and
    monitor the scrape of the web page. It handles simple errors due to connect-
    ion problems, and logs a range of information for basic quality assessments

    Keyword arguments:
    logfile          -- path to the logfile
    overwrite_log    -- bool, defining if logfile should be cleared (rarely the
                        case).
    connector_type   -- use the 'requests' module or the 'selenium'.
    session          -- requests.session object. For defining custom headers and
                        proxies.
    path2selenium    -- str, sets the path to the geckodriver needed when using
                        selenium.
    n_tries          -- int, defines the number of retries the *get* method will
                        try to avoid random connection errors.
    timeout          -- int, seconds the get request will wait for the server to
                        respond, again to avoid connection errors.
    """

    ## Selenium
    ## Initialization function defining parameters.
    self.n_tries = n_tries # number of attempts will be made per iteration
    self.timeout = timeout # maximum time to wait for a server to response.

    ## not implemented here, if you use selenium.
    if connector_type=='selenium':
        assert path2selenium!='',\
        "You need to specify the path to you geckodriver if you want to use Selenium"
        from selenium import webdriver

        assert os.path.isfile(path2selenium),'You need to insert a valid\
        path2selenium the path to your geckodriver. You can download the lates\
        geckodriver here: https://github.com/mozilla/geckodriver/releases'
        # start the browser with a path to the geckodriver.
        self.browser = webdriver.Firefox(executable_path=path2selenium)
    # set the connector_type
    self.connector_type = connector_type

    ## Requests
    if session: # set the custom session\
        self.session = session
    else:
        self.session = requests.session()
    self.logfilename = logfile # set the logfile path
    ## define header for the logfile
    header = ['id','project','connector_type','t', 'delta_t', 'url',\
              'redirect_url','response_size', 'response_code','success','error']
    if os.path.isfile(logfile):
        if overwrite_log==True:
            self.log = open(logfile,'w')
            self.log.write(';'.join(header))
        else:
            self.log = open(logfile,'a')
    else:
        self.log = open(logfile,'w')
        self.log.write(';'.join(header))

    ## load log
    with open(logfile,'r') as f: # open file
        l = f.read().split('\n') # read and split file by newlines.
        ## set id
        if len(l)<=1:
            self.id = 0
        else:
            self.id = int(l[-1][0])+1

  def get(self,url,project_name):
    """
    Method for connector reliably to the internet, with multiple tries and
    simple error handling, as well as default logging function. Input url and
    the project name for the log (i.e. is it part of mapping the domain, or is
    it the part of the final stage in the data collection).

    Keyword arguments:
    url              -- str, url
    project_name     -- str, Name used for analyzing the log. Use case could be
                        the 'Mapping of domain','Meta_data_collection','main
                        data collection'.
    """

    # make sure the default csv seperator is not in the project_name.
    project_name = project_name.replace(';','-')

    ## Determine connector method.
    ## Requests
    if self.connector_type=='requests':
        # for loop defining number of retries with the requests method.
        for _ in range(self.n_tries):
            ratelimit()
            t = time.time()

        try: # defines error handling
            # make get call to server
            response = self.session.get(url,timeout = self.timeout)

            ## Key arguments
            err = ''                    # define python error variable as empty assumming success.
            success = True              # define success variable
            redirect_url = response.url # log current url, after potential redirects
            dt = t - time.time()        # define delta-time waiting for the server and downloading content.
            size = len(response.text)   # define variable for size of html content of the response.
            response_code = response.status_code # log status code.
            ## log...
            call_id = self.id           # get current unique identifier for the call
            self.id+=1                  # increment call id

            # define row to be written in the log.
            row = [call_id, project_name, self.connector_type, t, dt, url,\
                   redirect_url, size, response_code, success, err]
            self.log.write('\n'+';'.join(map(str,row)))
            self.log.flush()

            # return response and unique identifier.
            return response, call_id

        # Defines Error output
        except Exception as e:          # define error condition
            err = str(e)                # python error
            response_code = ''          # blank response code
            success = False             # call success = False
            size = 0                    # content is empty.
            redirect_url = ''           # redirect url empty
            dt = t - time.time()        # define delta t

            ## logging scrape meta data
            call_id = self.id             # define unique identifier
            self.id+=1                    # increment call_id

            row = [call_id, project_name, self.connector_type, t, dt, url,
                   redirect_url, size, response_code, success, err]
            self.log.write('\n'+';'.join(map(str,row))) # write row to log.
            self.log.flush()

    ## Selenium
    else:
        t = time.time()
        ratelimit()
        self.browser.get(url) # use selenium get method

        try: #defines error handling
            """
            NOTE: several indicators are different than from the indicators
            that are established with the requests package. `dt`, does not
            necessarily reflect the complete load time. `size` might not  be
            correct as selenium works in the background and could still be
            loading
            """

            ## Key arguments
            err = ''                    # define python error variable as empty assumming success.
            success = True              # define success variable
            redirect_url = sel.browser.current_url # log current url, after potential redirects
            dt = t - time.time()        # define delta-time waiting for the server and downloading content.
            size = len(self.browser.page_source)   # define variable for size of html content of the response.
            response_code = ''          # log status code.
            ## log...
            call_id = self.id           # get current unique identifier for the call
            self.id+=1                  # increment call id

            # Defines row to be written in the log
            row = [call_id, project_name, self.connector_type, t, dt, url,\
                   redirect_url, size, response_code, success, err] # define row
            self.log.write('\n'+';'.join(map(str,row))) # write row to log file.
            self.log.flush()

            # Using selenium it will not return a response object, instead you
            # should call the browser object of the connector.
            ## connector.browser.page_source will give you the html.
            return None, call_id

        except Exception as e:          # define error condition
            err = str(e)                  # python error
            response_code = ''            # blank response code
            success = False               # call success = False
            size = 0                      # content is empty.
            redirect_url = ''             # redirect url empty
            dt = t - time.time()          # define delta t

            # Defines row to be written in the log
            row = [call_id, project_name, self.connector_type, t, dt, url,\
                   redirect_url, size, response_code, success, err] # define row
            self.log.write('\n'+';'.join(map(str,row))) # write row to log file.
            self.log.flush()
