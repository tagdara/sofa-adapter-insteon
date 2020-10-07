#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase, adapterbase, configbase
import devices

import math
import random
import requests
import json
import asyncio
import definitions
import aiohttp
import base64
from collections import defaultdict, Counter
import xml.etree.ElementTree as et
import urllib.request
import socket
import datetime

class insteonCatalog():
    
    def __init__(self, log=None, dataset=None, config=None):
        self.config=config
        self.dataset=dataset
        self.definitions=definitions.Definitions
        self.log=log
        self.basicauth="Basic %s" % base64.encodebytes(("%s:%s" % (self.config.isy_user,self.config.isy_password)).encode('utf-8')).decode()
        self.sema = asyncio.Semaphore(5)
        self.skip_nodetype=['root','folder']
        
    def etree_to_dict(self, t):
        
        d = {t.tag: {} if t.attrib else None}
        children = list(t)
        if children:
            dd = defaultdict(list)
            for dc in map(self.etree_to_dict, children):
                for k, v in dc.items():
                    dd[k].append(v)
            d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
        if t.attrib:
            d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
        if t.text:
            text = t.text.strip()
            if children or t.attrib:
                if text:
                    d[t.tag]['#text'] = text
            else:
                d[t.tag] = text
        return d

    async def authenticate(self):
        async with aiohttp.ClientSession() as client:
            self.payload='<s:Envelope><s:Body><u:Authenticate xmlns:u="urn:udi-com:service:X_Insteon_Lighting_Service:1"><name>%s</name><id>%s</id></u:Authenticate></s:Body></s:Envelope>\r\n' % (self.config.isy_user, self.config.isy_password)

            url = 'http://%s' % self.config.isy_address
            headers = { "Authorization": self.basicauth, 
                        "SOAPACTION": '"urn:udi-com:service:X_Insteon_Lighting_Service:1#Authenticate"'}

            return await client.post(url, data=self.payload, headers=headers)        

        
    async def getNodesList(self,client):
        
        try:
            self.log.info("GetNodesConfig Obtaining list of insteon nodes")
            self.nodesmaster={}
            url = "http://%s/rest/nodes/" % self.config.isy_address
            headers = { "Authorization": self.basicauth }
            data = await client.get(url, headers=headers)
            html = await data.read() 
            return html
        except:
            self.log.error("GetNodesList failed",exc_info=True)

    def getFolders(self, nodelist):
        
        folders=dict()
        for node in root.findall('folder'):
            address = node.find('address').text
            flag = node.get('flag')
            name = node.find('name').text
            try:
                parent = node.find('parent').text
            except:
                parent = '0000'

            folders[address]={"name":name,"parent":parent,"flag":flag,"address":address}
        
        for folder in folders:
            if folders[folder]['parent']=='0000':
                folders[folder]['parentname']='My Lighting'
            else:
                folders[folder]['parentname']=folders[folders[folder]['parent']]['name']
            self.log.debug("Folder discovered: "+str(folders[folder]))

        return folders
        
    async def getGroups(self,root):
        
        xmlgroups=[]
        groupprops=dict()

        for node in root.findall('group'):
            
            address = node.find('address').text
            flag = node.get('flag')
            name = node.find('name').text
            
            try:
                parent = node.find('parent').text
            except:
                parent = '0000'
                parentname = 'My Lighting'
                
            try:
                parentname=self.folders['folder'][parent]['name']
            except:
                parentname=parent
                
            xmlgroups.append([address,name])

            groupprops[address]=dict()
            groupprops[address]['address']=address
            groupprops[address]['name']=name
            groupprops[address]['parent']=parent
            groupprops[address]['parentname']=parentname
            self.log.debug("Group discovered: "+str(address)+":"+str(groupprops[address]))

        #return sorted(xmlgroups,key=itemgetter(1))
        return groupprops


    async def getNodeProperties(self, address):

        url="http://%s/rest/nodes/%s" % (self.config.isy_address, urllib.request.quote(address))
        headers = { "Authorization": self.basicauth }
        async with aiohttp.ClientSession() as client:
            async with self.sema, client.get(url, headers=headers) as data:
                html = await data.read()
        noderoot = et.fromstring(html)

        nodeprops={}
        for nodeproplist in noderoot.findall('properties'):
            for nodeprop in nodeproplist.findall('property'):
                #nodeprops[nodeprop.get("id")]=nodeprop.get("value")
                if nodeprop.get("id")!=None:
                    nodeprops[nodeprop.get("id")]=nodeprop.attrib  
                
        #self.log.info('Get props for '+str(address)+'='+str(nodeprops))
        return nodeprops


    async def main(self):
        async with aiohttp.ClientSession() as client:
            itemProperty=''
            html = await self.getNodesList(client)
            nodesJSON=self.etree_to_dict(et.fromstring(html))

            try:
                for nodetype in nodesJSON['nodes']:
                    if nodetype in self.skip_nodetype:
                        continue
                    if nodetype not in self.dataset.nativeDevices:
                        #self.log.info('prebake: %s before %s' % (nodetype,nodesJSON['nodes'][nodetype]))
                        self.dataset.nativeDevices[nodetype]={} # helps make sure the first added item uses the same format as subsequent
                    try:
                        for item in nodesJSON['nodes'][nodetype]:
                            try:
                                if 'enabled' in item:
                                    if item['enabled']=="false":
                                        self.log.info('Skipping disabled device: %s' % (item['name']))
                                        continue
                                if 'property' in item:
                                    item['property']=await self.getNodeProperties(item['address']) # Replace subset of property with complete set
                                    if item['address'] in self.dataset.config.device_override:
                                        item['devicetype']=self.dataset.config.device_override[item['address']]
                                        self.log.info('.. Device type manually set for %s: %s' % (item['address'],item['devicetype']))
                                    else:
                                        item['devicetype']=self.definitions.deviceTypes[item['type']]

                                    if item['devicetype']=='light':
                                        # fix for multi-button keypads
                                        if (item['pnode']!=item['address']):
                                            item['devicetype']='button'
                                    if item['devicetype'] in ['lightswitch','button']:
                                        item['pressState']='none'

                            except:
                                self.log.error('couldnt get property or find device type for: %s / %s' % (item['address'],item['type']), exc_info=True)
                            await self.dataset.ingest({ nodetype: { item['address']:  item }})
                    except TypeError:
                        self.log.warn('No data in node type: %s' % nodetype)
                    except:
                        self.log.error('Couldnt walk node type %s: %s' % (nodetype,itemProperty), exc_info=True)

            except:
                self.log.error('Error with main', exc_info=True)
            


class insteonSubscription(asyncio.Protocol):

    def __init__(self, loop, log=None, notify=None, dataset=None, config=None, **kwargs):
        self.config=config
        self.dataset=dataset
        self.is_open = False
        self.loop = loop
        self.log=log
        self.last_message = ""
        self.definitions=definitions.Definitions
        self.sendQueue=[]
        self.notify=notify
        self.eventdata=''
        self.logbusy=False
        self.catalog=insteonCatalog
        self.controllerBusy=False
        self.basicauth="Basic %s" % base64.encodebytes(("%s:%s" % (self.config.isy_user,self.config.isy_password)).encode('utf-8')).decode()
        self.pendingChanges=[]
        self.pending_data={}

    def etree_to_dict(self, t):
        
        d = {t.tag: {} if t.attrib else None}
        children = list(t)
        if children:
            dd = defaultdict(list)
            for dc in map(self.etree_to_dict, children):
                for k, v in dc.items():
                    dd[k].append(v)
            d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
        if t.attrib:
            d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
        if t.text:
            text = t.text.strip()
            if children or t.attrib:
                if text:
                    d[t.tag]['#text'] = text
            else:
                d[t.tag] = text
        return d


    async def getNodeProperties(self, address):

        try:
            url="http://%s/rest/nodes/%s" % (self.config.isy_address, urllib.request.quote(address))
            headers = { "Authorization": self.basicauth }
            async with aiohttp.ClientSession() as client:
                async with self.sema, client.get(url, headers=headers) as data:
                    html = await data.read()
                    noderoot = et.fromstring(html)

            nodeprops={}
            for nodeproplist in noderoot.findall('properties'):
                for nodeprop in nodeproplist.findall('property'):
                    #nodeprops[nodeprop.get("id")]=nodeprop.get("value")
                    if nodeprop.get("id")!=None:
                        nodeprops[nodeprop.get("id")]=nodeprop.attrib  
                
            #self.log.info('Get props for '+str(address)+'='+str(nodeprops))
            return nodeprops
        except:
            self.log.error('Error getting node properties: %s' % url, exc_info=True)

    def set_debug_level(self, level):

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect((self.config.isy_address, self.config.isy_port))
            sock.settimeout(10.0)

            logstr="<s:Envelope><s:Body><u:SetDebugLevel><option>"+level+"</option></u:SetDebugLevel></s:Body></s:Envelope>"
            loghead="POST /services HTTP/1.1\r\n"
            loghead+="Host: %s:%u\r\n" % (self.config.isy_address, self.config.isy_port)
            loghead+="Content-Length: %u\r\n" % len(logstr)
            loghead+="Content-Type: text/xml; charset=\"utf-8\"\r\n"	
            loghead+="Authorization: "+ self.basicauth
            loghead+="SOAPACTION:\"urn:udi-com:service:X_Insteon_Lighting_Service:1#SetDebugLevel\"\r\n\r\n"
            loghead+=logstr+"\r\n"
            sock.send(loghead.encode())
            answer = sock.recv(1024).decode()
            answer = sock.recv(1024).decode()
            sock.close()
            event=self.etree_to_dict(et.fromstring(answer))
            result=event['{http://www.w3.org/2003/05/soap-envelope}Envelope']['{http://www.w3.org/2003/05/soap-envelope}Body']['UDIDefaultResponse']['status']
            if result=='200':
                self.log.info('.. ISY controller debug level now set to %s' % level)
                return True
            else:
                self.log.error('!. Error setting ISY controller debug level level: %s' % answer)
                return False
            # print "Set Log Level response: "+answer
            #if (answer.find("HTTP/1.1 200 OK") == -1):
            #    self.adapter.forwardevent(action="info",data="Could not set log level")
            #    self.adapter.forwardevent(action="info",data=answer.strip())
            #    return False
            #else:
            #    self.log.info("Log level set to "+str(level))
            #    #self.adapter.forwardevent(action="info",data="Log level set to "+level)
            #    return True
        except:
            self.log.error('Error setting ISY controller debug level',exc_info=True)
            return False        
            
    def get_debug_level(self):

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect((self.config.isy_address,self.config.isy_port))
            sock.settimeout(10.0)
            logstr="<s:Envelope><s:Body><u:GetDebugLevel /></s:Body></s:Envelope>"
            loghead="POST /services HTTP/1.1\r\n"
            loghead+="Host: %s:%u\r\n" % (self.config.isy_address,self.config.isy_port)
            loghead+="Content-Length: %u\r\n" % len(logstr)
            loghead+="Content-Type: text/xml; charset=\"utf-8\"\r\n"	
            loghead+="Authorization: "+ self.basicauth
            loghead+="SOAPACTION:\"urn:udi-com:service:X_Insteon_Lighting_Service:1#GetDebugLevel\"\r\n\r\n"
            loghead+=logstr+"\r\n"
            sock.send(loghead.encode())
            answer = sock.recv(1024).decode()
            answer = sock.recv(1024).decode()
            sock.close()
            event=self.etree_to_dict(et.fromstring(answer))
            try:
                level=int(event['{http://www.w3.org/2003/05/soap-envelope}Envelope']['{http://www.w3.org/2003/05/soap-envelope}Body']['DBG']['current'])
                self.log.info('.. ISY controller debug level is %s' % level)
            except:
                self.log.error('.! Error getting debug level', exc_info=True)
                level=0
            return level
        except xml.etree.ElementTree.ParseError:
            self.log.error('!! Error getting ISY debug level - bad data in response: %s' % answer)
        except:
            self.log.error('!! Error getting ISY debug level',exc_info=True)
        return False        
               
    def sendSubscribeRequest(self, transport):
        try:
            subscribestr="<s:Envelope><s:Body><u:Subscribe xmlns:u=\"urn:udi-com:service:X_Insteon_Lighting_Service:1\"></u:Subscribe></s:Body></s:Envelope>"
            connecthead="SUBSCRIBE /eventing HTTP/1.1\r\n"
            connecthead+="Host: %s:%u\r\n" % (self.config.isy_address, self.config.isy_port)
            connecthead+="Content-Length: %u\r\nContent-Type: text/xml; charset=\"utf-8\"\r\n" % len(subscribestr)
            connecthead+="CALLBACK:<REUSE_SOCKET>\r\nNT:upnp:event\r\nTIMEOUT:Second-infinite\r\n"
            connecthead+="Authorization: %s" % self.basicauth
            connecthead+="SOAPACTION:\"urn:udi-com:service:X_Insteon_Lighting_Service:1#Subscribe\"\r\n\r\n"
            connecthead+=subscribestr+"\r\n"
            transport.write(connecthead.encode())
        except:
            self.log.error('Error subscribing to ISY', exc_info=True)

   
    def connection_made(self, transport):
        
        try:
            self.sockname = transport.get_extra_info("sockname")
            self.transport = transport
            self.is_open = True
            #self.log.info('Sending log level request')
            if self.get_debug_level() < 3:
                self.set_debug_level('3')
            self.log.info('Sending subscription request')
            self.sendSubscribeRequest(self.transport)
            self.sema = asyncio.Semaphore(5)

        except:
            self.log.error('Insteon Subscriber Connection made but something went wrong.', exc_info=True)
        

    def connection_lost(self, exc):
        self.log.info('Insteon Subscription Connection lost')
        self.is_open = False
        self.loop.stop()
        
        
    def send(self, data):
        
        try:
            self.transport.write(data.encode())
            self.log.info('> insteon  %s' % data)
        except:
            self.log.error('Error on send', exc_info=True)


    def data_received(self, data):
        
        #self.log.info('insteon > (%s)' % data)
        self.eventdata=self.eventdata+data.decode()
        
        while (self.eventdata.find("<Event") > -1):
            try:
                event=self.eventdata[self.eventdata.find("<Event"):self.eventdata.find("</Event>")+8]
                self.eventdata=self.eventdata[self.eventdata.find("</Event>")+8:]
                #self.log.info('insteon > (%s)' % event)
                asyncio.ensure_future(self.process_event(event))
            except:
                self.log.error('Error on receive: %s' % self.eventdata, exc_info=True)


    async def process_event(self, eventdata):
        
        try:
            event=self.etree_to_dict(et.fromstring(eventdata))['Event']
            #self.log.info('event: %s' % event)
        except:
            self.log.error('Error parsing event to dict: %s' % eventdata, exc_info=True)
            return None
            
        try:
            if event['control']=='_0':
                self.process_heartbeat(event)
            elif event['control']=='_1':
                await self.process_triggerevents(event)
            elif event['control']=='_3':
                self.process_nodechanged(event)
            elif event['control']=='_4':
                self.log.debug('Systems setting change reported: %s' % event['action'])
            elif event['control']=='_5':
                self.process_busystate(event)
                self.log.debug('Busy state command: %s' % event)
                if event['action']=='1':
                    self.controllerBusy=True
                if event['action']=='0':
                    self.controllerBusy=False
            elif event['control']=="_7":  # Progress Report.  Whatever that is.
                self.log.debug("Progress Report: %s " % event['action'])

            elif event['control']=="_19": # Elk commands per https://www.universal-devices.com/developers/wsdk/3.3.1/ISY-WS-SDK-ELK.pdf
                pass

            elif event['control']=='ERR':
                if event['action']!='0':
                    self.log.info('Node is in error state: %s (%s)' % (event['node'], event['action']))
                else:
                    self.log.debug('Node is in cleared error state: %s (%s)' % (event['node'], event['action']))
                
            elif event['control'] in ['RR','OL','ST','UOM','CLIFS','CLIHCS','CLIHUM','CLIMD','CLISPC','CLISPH']:
                
                # These controls seem to hit before the actual device is updated, so the follow-on REST check
                # comes back with the existing data and nothing happens.  A later update occurs with a _3 node change to 
                # confirm the node has changed. 
                # Since this issues a request for each message that is wasted, consider eliminating this altogether,
                # but confirm that all functionality is handled by a _3 (thermostats, etc)
                
                # Note that _3 commands only appear when the log level is set high enough, and that the ISY does not report
                # when the log level has been lowered under a number of circumstances
                
                # TODO: use this as a check for expected _3 messages and reset the log level if they do not arrive
                # in a reasonable amount of time.
                
                try:
                    try:
                        #self.log.info('Property event: %s %s' % (event['node'],event))
                        #self.log.info('Existing data: %s' % self.dataset.nativeDevices['node'][event['node']])
                        if event['control'] in self.dataset.nativeDevices['node'][event['node']]['property']:
                            if self.dataset.nativeDevices['node'][event['node']]['property'][event['control']]['value']==event['action']:
                                return None
                    except:
                        self.log.error('Error comparing previous state', exc_info=True)
                    #self.log.info('Property event: %s %s' % (event['node'],event))
                    #self.log.info('Existing data: %s %s' % (event['node'], self.dataset.nativeDevices['node'][event['node']]))
                    if self.pendingChanges:
                        self.log.info('.. pending %s' % self.pendingChanges)
                        
                    # TODO/Cheese - this causes a bunch of problems when combined with handling groups
                    # I'm temporarily removing the pending changes check to see what happens but this is probably
                    # needed for other things
                    
                    #if event['node'] not in self.pendingChanges:
                    if 1==1:
                        # testing allowing the _1 update to provide the update when the change was requested by the UI
                        updatedProperties=await self.getNodeProperties(event['node'])
                        #self.log.info('event UpdatedProperties: %s not in pending %s - %s' % (event['node'], self.pendingChanges, updatedProperties))
                        changeReport=await self.dataset.ingest({'node': { event['node']: {'property':updatedProperties}}})
                        if changeReport:
                            self.log.debug('event changereport: %s' % changeReport)
                    else:
                        updatedProperties=await self.getNodeProperties(event['node'])
                        self.pending_data[event['node']]=updatedProperties
                        self.log.info('Update processing deferred to change: %s %s' %  (updatedProperties, self.dataset.nativeDevices['node'][event['node']]))

                except TimeoutError:
                    self.log.error('Timeout accessing ISY for node properties: %s' % event['node'])
                except:
                    self.log.error('Error updating node: %s (%s)' % (event['node'], eventdata), exc_info=True)

            else: 
                self.log.info('Unprocessed response: %s (%s)' % (event, eventdata))
        except:
            self.log.warn('No control found in %s' % event,exc_info=True)


    def process_heartbeat(self, event):
        
        try:
            self.log.debug('Heartbeat with interval of %s seconds' % event['action'])
        except:
            self.log.warn('Error processing heartbeat: %s' % event,exc_info=True)


    def process_busystate(self, event):

        try:
            self.log.debug('insteon controller > %s' % self.definitions.busyStates[event['action']])
        except:
            self.log.warn('Error processing busy state: %s' % event,exc_info=True)
            
    async def process_triggerevents(self, event):
        
        try:
            self.log.debug('Trigger Event > %s > %s' % (self.definitions.triggerEvents[event['action']], event['eventInfo']))
        except:
            self.log.warn('Error processing trigger event %s' % event,exc_info=True)

        try:
            #self.log.info('<[ %s %s ' % (event['action'],event))
            if event['action']=='3':
                vEvent={}
                vEvent['node']=event['eventInfo'][event['eventInfo'].find("[")+1:event['eventInfo'].find("]")].strip()
                try:
                    vEvent['name']=self.dataset.nativeDevices['node'][vEvent['node']]['name']
                except:
                    pass
                    
                vEvent['cmd']=event['eventInfo'][event['eventInfo'].find("]")+1:].strip()
                if vEvent['node'].find('VAR')>-1:
                    vEvent['var']=vEvent['node'].split()[1]
                    vEvent['val']=vEvent['node'].split()[2]
                    self.log.info(".. insteon Variable %s set to %s" % (vEvent['var'], vEvent['val']))
                elif vEvent['node']=="Time":
                    self.log.debug("Time info: %s %s" % (vEvent['node'], event['eventInfo']))
                else:
                    vEvent['val']=int(vEvent['cmd'].split()[1])
                    vEvent['command']=vEvent['cmd'].split()[0]
                    self.log.info('<[ %s' % vEvent)
                    if vEvent['node'] in self.pendingChanges:
                        self.pendingChanges.remove(vEvent['node'])
                    if vEvent['command'] in ['DON','DOF','DFOF','DFON']:
                        #self.log.info('Button command received on Insteon Bus: %s' % vEvent)
                        #self.log.info('Current native: %s' % self.dataset.nativeDevices['node'][vEvent['node']])
                        await self.dataset.ingest({'node': { vEvent['node'] : {'pressState':vEvent['command'] }}})
                        await self.dataset.ingest({'node': { vEvent['node'] : {'pressState':'none' }}})                          
        except:
            self.log.error('Error handling trigger event: %s' % event, exc_info=True)


    def process_nodechanged(self, event):
        
        try:
            if self.definitions.nodeChanges[event['action']]=="Node Error (Comm. Errors)":
                if self.dataset.nativeDevices['node'][event['node']]['type'] in self.definitions.wirelessDeviceTypes:
                    self.log.debug('.. Node Changed > %s (wireless) > %s' % (self.definitions.nodeChanges[event['action']], event['node']))
                    return None
                    
            self.log.info('.. Node Changed > %s > %s' % (self.definitions.nodeChanges[event['action']], event['node']))
            if event['node'] in self.pendingChanges:
                self.pendingChanges.remove(event['node'])
        except:
            self.log.warn('Error processing Node Change: %s' % event,exc_info=True)
        
    
    # --------------------------------------------------------------------------    
      
class insteonSetter():
    
    def __init__(self, log, dataset=None, config=None):
        self.config=config
        self.dataset=dataset
        self.log=log
        self.basicauth="Basic %s" % base64.encodebytes(("%s:%s" % (self.config.isy_user,self.config.isy_password)).encode('utf-8')).decode()

    async def insteonRestCommand(self, client, url):
        
        try:
            headers = { "Authorization": self.basicauth }
            data = await client.get(url, headers=headers)
            html = await data.read() 
            return html

        except:
            self.log.error("insteonRestCommand failed: %s" % url,exc_info=True)


    async def setNode(self, node, data):
        
        try:
            set_start=datetime.datetime.now()
            self.log.info('.. Setting node: %s %s' % (node, data))
            if node not in self.dataset.nativeDevices['node']:
                for alight in self.dataset.nativeDevices['node']:
                    if self.dataset.nativeDevices['node'][alight]['name']==node:
                        node=alight
                        break
                
            for nodeattrib in data:
                if nodeattrib.upper() in ['DON','DOF','DFON', 'DFOF']:
                    if nodeattrib.upper() in ['DON', 'DFON'] and data[nodeattrib]:
                        url="http://%s/rest/nodes/%s/%s/%s/%s" % (self.config.isy_address, node, "cmd", nodeattrib, data[nodeattrib])
                    else:
                        url="http://%s/rest/nodes/%s/%s/%s" % (self.config.isy_address, node, "cmd", nodeattrib)
                elif nodeattrib.upper()=='ST':
                    if int(data[nodeattrib])==0:
                        control='DOF'
                    else:
                        control='DON'
                    url="http://%s/rest/nodes/%s/%s/%s/%s" % (self.config.isy_address, node, "cmd", control, data[nodeattrib])
                else:
                    url="http://%s/rest/nodes/%s/%s/%s/%s" % (self.config.isy_address, node, "set", nodeattrib.upper(), data[nodeattrib])
                
            #self.log.info('Using url: %s' % url)
            async with aiohttp.ClientSession() as client:
                html = await self.insteonRestCommand(client, url)
                root=et.fromstring(html)
                self.log.info('.. returned from using url: %s / %s %s' % (url, html, root))
                try:
                    if root.attrib['succeeded']=='true':
                        return True
                except:
                    self.log.error('!! error in html response: %s' % html, exc_info=True)
            return False
            
        except concurrent.futures._base.CancelledError:
            self.log.error('Insteon setNode error (cancelled after %ss): %s %s' % ((datetime.datetime.now()-set_start).total_seconds(), node, data))
        except:
            self.log.error('Insteon setNode error (after %ss): %s %s' % ((datetime.datetime.now()-set_start).total_seconds(), node, data), exc_info=True)


    async def setGroup(self, group, data):
        
        try:
            self.log.info('Setting Group: %s %s' % (group, data))
            if group not in self.dataset.nativeDevices['group']:
                for agroup in self.dataset.nativeDevices['group']:
                    if self.dataset.nativeDevices['group'][agroup]['name']==group:
                        group=agroup
                        break
                
            for groupattrib in data:
                if groupattrib.upper()=='ST':
                    if int(data[groupattrib])==0:
                        control='DOF'
                    else:
                        control='DON'
                    url="http://%s/rest/nodes/%s/%s/%s/%s" % (self.config.isy_address, group, "cmd", control, data[groupattrib])
                else:
                    url="http://%s/rest/nodes/%s/%s/%s/%s" % (self.config.isy_address, group, "set", groupattrib.upper(), data[groupattrib])
                
                self.log.info('Using url: %s' % url)
                async with aiohttp.ClientSession() as client:
                    html = await self.insteonRestCommand(client, url)
                    root=et.fromstring(html)
                    return root
        except:
            self.log.error('Insteon setGroup error: %s %s' % (group, data), exc_info=True)


        
class insteon(sofabase):

    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.isy_address=self.set_or_default('isy_address', mandatory=True)
            self.isy_port=self.set_or_default('isy_port', default=80)
            self.isy_user=self.set_or_default('isy_user', default="admin")
            self.isy_password=self.set_or_default('isy_password', mandatory=True)
            self.device_override=self.set_or_default('device_override',default={})

    class EndpointHealth(devices.EndpointHealth):

        @property            
        def connectivity(self):
            return 'OK'

    class PowerController(devices.PowerController):

        @property            
        def powerState(self):
            if self.nativeObject['property']['ST']['value']==' ':
                return "OFF"
            return "ON" if int((float(self.nativeObject['property']['ST']['value'])/254)*100)>0 else "OFF"

        async def TurnOn(self, correlationToken='', **kwargs):
            try:
                tol=255
                #if int((float(self.nativeObject['property']['ST']['value'])/254)*100)>0:
                #    tol=int((float(self.nativeObject['property']['ST']['value'])/254)*100)
                try:
                    tol=int(self.nativeObject['property']['OL']['value'])
                except:
                    self.log.error('!. couldnt get onlevel for %s' % self.nativeObject)
                return await self.adapter.setAndUpdate(self.device, {'DON': tol}, "powerState", "ON", correlationToken, self)
            except:
                self.adapter.log.error('!! Error during TurnOn', exc_info=True)
        
        async def TurnOff(self, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'DOF': 0}, "powerState", "OFF", correlationToken, self)
            except:
                self.adapter.log.error('!! Error during TurnOff', exc_info=True)
                
    class BrightnessController(devices.BrightnessController):

        @property            
        def brightness(self):
            # sometimes switches have their ST value set to space instead of a number (shrug)
            if self.nativeObject['property']['ST']['value']==' ':
                try:
                    return int((float(self.nativeObject['property']['OL']['value'])/254)*100)
                except:
                    return 0
                    
            # Return the on-level brightness if the device is off and it is available, similar to Hue lights
            if int((float(self.nativeObject['property']['ST']['value'])/254)*100)==0:
                try:
                    return int((float(self.nativeObject['property']['OL']['value'])/254)*100)
                except:
                    pass
            
            return int((float(self.nativeObject['property']['ST']['value'])/254)*100)
        
        async def SetBrightness(self, payload, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'ST' : self.adapter.percentage(int(payload['brightness']), 255) }, "brightness", payload['brightness'], correlationToken, self)
            except:
                self.adapter.log.error('!! Error setting brightness', exc_info=True)

    class SwitchController(devices.SwitchController):

        @property            
        def pressState(self):
            # sometimes switches have their ST value set to space instead of a number (shrug)
            if self.nativeObject['pressState'] in ['DON','DFON']:
                return "ON"
            elif self.nativeObject['pressState'] in ['DOF','DFOF']:
                return "OFF"
            return "NONE"

        @property            
        def onLevel(self):
            if 'OL' not in self.nativeObject['property']:
                return 100
            if self.nativeObject['property']['OL']['value']==' ':
                return {}
            return  int((float(self.nativeObject['property']['OL']['value'])/254)*100)
   
        async def SetOnLevel(self, payload, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'ST' : self.adapter.percentage(int(payload['brightness']), 255) }, "brightness", payload['brightness'], correlationToken, self)
            except:
                self.adapter.log.error('!! Error setting brightness', exc_info=True)

        async def SetOnLevel(self, payload, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'OL' : self.adapter.percentage(int(payload['onLevel']), 255) }, "onLevel", payload['onLevel'], correlationToken, self)
            except:
                self.adapter.log.error('!! Error setting onLevel', exc_info=True)
                return None

    class TemperatureSensor(devices.TemperatureSensor):

        @property            
        def temperature(self):
            if self.nativeObject['property']['ST']['value']==' ':
                return None
            return int(float(self.nativeObject['property']['ST']['value'])/2)
            
    class ThermostatController(devices.ThermostatController):

        @property            
        def targetSetpoint(self):                
            if self.nativeObject['property']['CLISPH']['value']==' ':
                return None
            return int(float(self.nativeObject['property']['CLISPH']['value'])/2)

        @property            
        def thermostatMode(self):                
            if self.nativeObject['property']['CLIMD']['formatted']==' ':
                return "OFF"
            return self.nativeObject['property']['CLIMD']['formatted'].upper()

  
        async def SetTargetTemperature(self, payload, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, { 'CLISPH' : int(payload['targetSetpoint']['value'])*2 }, "targetSetpoint", payload['targetSetpoint']['value'], correlationToken, self)
            except:
                self.adapter.log.error('!! Error setting target temperature', exc_info=True)

        async def SetThermostatMode(self, payload, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, { 'CLIMD' : self.adapter.definitions.thermostatModesByName[payload['thermostatMode']['value']] }, "thermostatMode", payload['thermostatMode']['value'], correlationToken, self)
            except:
                self.adapter.log.error('!! Error setting thermostat mode', exc_info=True)

    class GroupPowerController(devices.PowerController):

        @property            
        def powerState(self):
            return None
            
        async def TurnOn(self, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'DON': ''}, "powerState", "ON", correlationToken, self)
            except:
                self.adapter.log.error('!! Error during TurnOn', exc_info=True)
        
        async def TurnOff(self, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'DOF': 0}, "powerState", "OFF", correlationToken, self)
            except:
                self.adapter.log.error('!! Error during TurnOff', exc_info=True)
                
    class GroupBrightnessController(devices.BrightnessController):

        @property            
        def brightness(self):
            return None
        
        async def SetBrightness(self, payload, correlationToken='', **kwargs):
            try:
                return await self.adapter.setAndUpdate(self.device, {'ST' : self.adapter.percentage(int(payload['brightness']), 255) }, "brightness", payload['brightness'], correlationToken, self)
            except:
                self.adapter.log.error('!! Error setting brightness', exc_info=True)



    class adapterProcess(adapterbase):

        def __init__(self, log=None, dataset=None, notify=None, request=None, loop=None, config=None, **kwargs):
            self.config=config
            self.dataset=dataset
            self.log=log
            self.definitions=definitions.Definitions
            self.notify=notify
            self.insteonNodes=insteonCatalog(self.log, self.dataset, config=self.config)
            self.setInsteon=insteonSetter(self.log, dataset=self.dataset, config=self.config)
            if not loop:
                self.loop = asyncio.new_event_loop()
            else:
                self.loop=loop
            
        async def pre_activate(self):
            
            try:
                self.insteonNodes.data=self.dataset.nativeDevices
                self.setInsteon.data=self.dataset.nativeDevices
                await self.insteonNodes.authenticate()
                await self.insteonNodes.main()
                self.log.info('----')
                
                self.subscription = insteonSubscription(self.loop, self.log, self.notify, self.dataset, config=self.config)
                await self.loop.create_connection(lambda: self.subscription, self.config.isy_address, self.config.isy_port)
            except:
                self.log.error('.. pre-activate error', exc_info=True)


            
        async def start(self):
            
            self.log.info('Starting Insteon')



        def percentage(self, percent, whole):
            return int((percent * whole) / 100.0)


        async def addSmartDevice(self, path):
            
            # All Insteon Smart Devices will be nodes except groups
            if path.split("/")[1]!='node' and path.split("/")[1]!='group':
                return False
            
            deviceid=path.split("/")[2]    
            nativeObject=self.dataset.getObjectFromPath(self.dataset.getObjectPath(path))
            if 'deviceGroup' in nativeObject:
                endpointId="%s:%s:%s" % ("insteon","group", deviceid)
            else:
                endpointId="%s:%s:%s" % ("insteon","node", deviceid)
            
            if 'name' in nativeObject and endpointId not in self.dataset.localDevices:
                if 'devicetype' in nativeObject:
                    if nativeObject["devicetype"]=="light":
                        device=devices.alexaDevice('insteon/node/%s' % deviceid, nativeObject['name'], displayCategories=['LIGHT'], adapter=self)
                        device.PowerController=insteon.PowerController(device=device)
                        device.EndpointHealth=insteon.EndpointHealth(device=device)
                        device.StateController=devices.StateController(device=device)
                        if nativeObject["property"]["ST"]["uom"].find("%")>-1:
                            device.BrightnessController=insteon.BrightnessController(device=device)
                        return self.dataset.add_device(device)
    
                    elif nativeObject["devicetype"]=="lightswitch":
                        device=devices.alexaDevice('insteon/node/%s' % deviceid, nativeObject['name'], displayCategories=['LIGHT'], adapter=self)
                        device.PowerController=insteon.PowerController(device=device)
                        device.EndpointHealth=insteon.EndpointHealth(device=device)
                        device.StateController=devices.StateController(device=device)
                        device.SwitchController=insteon.SwitchController(device=device)
                        if nativeObject["property"]["ST"]["uom"].find("%")>-1:
                            device.BrightnessController=insteon.BrightnessController(device=device)
                        return self.dataset.add_device(device)
    
                    elif nativeObject["devicetype"]=="button":
                        device=devices.alexaDevice('insteon/node/%s' % deviceid, nativeObject['name'], displayCategories=['SWITCH'], adapter=self)
                        device.SwitchController=insteon.SwitchController(device=device)
                        return self.dataset.add_device(device)
    
                    elif nativeObject["devicetype"]=="thermostat":
                        if nativeObject['pnode']==deviceid:
                            device=devices.alexaDevice('insteon/node/%s' % deviceid, nativeObject['name'], displayCategories=['THERMOSTAT'], adapter=self)
                            device.ThermostatController=insteon.ThermostatController(device=device)
                            device.TemperatureSensor=insteon.TemperatureSensor(device=device)
                            return self.dataset.add_device(device)
    
                    elif nativeObject["devicetype"]=="device":
                        device=devices.alexaDevice('insteon/node/%s' % deviceid, nativeObject['name'], displayCategories=['SWITCH'], adapter=self)
                        device.PowerController=insteon.PowerController(device=device)
                        return self.dataset.add_device(device)
                    
                elif 'deviceGroup' in nativeObject:
                    device=devices.alexaDevice('insteon/group/%s' % deviceid, nativeObject['name'], displayCategories=['GROUP'], adapter=self, hidden=True)
                    device.EndpointHealth=insteon.EndpointHealth(device=device)    
                    device.PowerController=insteon.GroupPowerController(device=device)
                    #device.BrightnessController=insteon.GroupBrightnessController(device=device)
                    return self.dataset.add_device(device)
                    
            return False
            
        def compare_native(self, native_device, native_command):

            try:
                if 'property' not in native_device:
                    return False
                device_command = next(iter(native_command.keys()))
                if device_command in ['DON','DOF','DFON','DFOF']:
                    device_prop='ST'
                else:
                    device_prop=device_command
                if device_prop in native_device['property']:
                    if int(native_device['property'][device_prop]['value'])==int(native_command[device_command]):
                        self.log.info('.. compare_native %s/%s matched %s:%s / %s:%s' % (native_device['name'], native_device['address'], device_prop, native_device['property'][device_prop]['value'], device_command, native_command[device_command]))
                        return True
                else:
                    self.log.info('%s not in %s' % (device_prop, native_device['property']))
            except:
                self.log.error('!! error matching native', exc_info=True)
                
            #self.log.info('.. compare_native %s/%s no match %s:%s / %s:%s' % (native_device['name'], native_device['address'], device_prop, native_device['property'][device_prop]['value'], device_command, native_command[device_command]))
            return False


        async def setAndUpdate(self, device, command, controllerprop, controllervalue, correlationToken, controller):
            
            #  General Set and Update process for insteon. Most direct commands should just set the native command parameters
            #  and then call this to apply the change
            
            # TODO/Cheese - there is blending of the native properties and Alexa property names that is screwing up the 
            # overall data and should be reviewed.
            
            try:
                groupmembers=[]
                deviceid=self.dataset.getNativeFromEndpointId(device.endpointId)
                if self.compare_native(device.native, command):
                    self.log.info('.. device already set: %s %s %s=%s' % (controller.controller, controllerprop, getattr(controller, controllerprop), controllervalue))
                    #await self.dataset.ingest({'node': { deviceid: {'property': { device_prop : { "value" : command[device_command] }}}}})
                    return device.Response(correlationToken)
                #self.log.info('command: %s' % command)

                if device.endpointId.startswith('insteon:group:'):
                    groupmembers=await self.group_members(device.endpointId)
                    for member in groupmembers:
                        short_member=member.split(':')[2]
                        #if short_member not in self.subscription.pendingChanges:
                            #self.log.info('.. adding group member to pending: %s' % short_member)
                            #self.subscription.pendingChanges.append(short_member)
                else:
                    if deviceid not in self.subscription.pendingChanges:
                        self.subscription.pendingChanges.append(deviceid)
                    
                        
                result=await self.setInsteon.setNode(deviceid, command)
                if result:
                    self.log.info('.. assuming success on response. pre-setting %s to %s' % (controllerprop, controllervalue) )
                    updatedProperties={ controllerprop : { "value": controllervalue }}
                    device_command = next(iter(command.keys()))
                    # Change direct command to prop
                    if device_command in ['DON','DOF','DFON','DFOF']:
                        device_prop='ST'
                    else:
                        device_prop=device_command
                    
                    # Split out native groups to devices
                    if device.endpointId.startswith('insteon:group:'):
                        groupmembers=await self.group_members(device.endpointId)
                        for member in groupmembers:
                            short_member=member.split(':')[2]
                            await self.dataset.ingest({'node': { short_member: {'property': { device_prop : { "value": command[device_command] }}}}})
                            asyncio.create_task(self.follow_up_on_status(device, command, controllerprop, controllervalue, short_member, controller))
                    else:
                        await self.dataset.ingest({'node': { deviceid: {'property': { device_prop : { "value": command[device_command] }}}}})
                        asyncio.create_task(self.follow_up_on_status(device, command, controllerprop, controllervalue, deviceid, controller))
                    return device.Response(correlationToken)  
                    
                if device.endpointId.startswith('insteon:group:'):
                    for member in groupmembers:
                        short_member=member.split(':')[2]
                        #await self.waitPendingChange(short_member, no_add=True)   
                        #updatedProperties=await self.insteonNodes.getNodeProperties(short_member)
                        #await self.dataset.ingest({'node': { short_member: {'property':updatedProperties}}})
                else:
                    #self.log.info('Comparing %s vs %s' % (getattr(controller, controllerprop),controllervalue))
                    if getattr(controller, controllerprop)!=controllervalue:
                        await self.waitPendingChange(deviceid)
                
                if deviceid in self.subscription.pending_data:
                    self.log.info('.. getting pending data for %s : %s' % (deviceid, self.subscription.pending_data[deviceid]))
                    updatedProperties=dict(self.subscription.pending_data[deviceid])
                    del self.subscription.pending_data[deviceid]
                else:
                    self.log.debug('.. no pending change data for %s' % (deviceid))
                    updatedProperties=await self.insteonNodes.getNodeProperties(deviceid)
                if not device.endpointId.startswith('insteon:group:'):
                    await self.dataset.ingest({'node': { deviceid: {'property':updatedProperties}}})
                else:
                    await self.dataset.ingest({'group': { deviceid: {'property':updatedProperties}}})
                return device.Response(correlationToken)
            except:
                self.log.error('!! Error during Set and Update: %s %s / %s %s' % (deviceid, command, controllerprop, controllervalue), exc_info=True)
                return None

        async def follow_up_on_status(self, device, command, controllerprop, controllervalue, deviceid, controller):
            try:
                if not device.endpointId.startswith('insteon:group:'):
                    if getattr(controller, controllerprop)!=controllervalue:
                        await self.waitPendingChange(deviceid)
                
                if deviceid in self.subscription.pending_data:
                    self.log.info('.. getting pending data for %s : %s' % (deviceid, self.subscription.pending_data[deviceid]))
                    updatedProperties=dict(self.subscription.pending_data[deviceid])
                    del self.subscription.pending_data[deviceid]
                else:
                    self.log.debug('.. no pending change data for %s' % (deviceid))
                    updatedProperties=await self.insteonNodes.getNodeProperties(deviceid)
                if not device.endpointId.startswith('insteon:group:'):
                    await self.dataset.ingest({'node': { deviceid: {'property':updatedProperties}}})
                else:
                    await self.dataset.ingest({'group': { deviceid: {'property':updatedProperties}}})

            except:
                self.log.error('!! error following up on presumed status: %s %s / %s %s' % (deviceid, command, controllerprop, controllervalue), exc_info=True)


        async def waitPendingChange(self, deviceid, maxcount=60, no_add=False):
        
            count=0
            try:
                # The ISY will send an update to the properties, but it takes .5-1 second to complete
                # Waiting up to 2 seconds allows us to send back a change report for the change command
                if not no_add:
                    if deviceid not in self.subscription.pendingChanges:
                        self.subscription.pendingChanges.append(deviceid)
                        self.log.info('.. waiting for update... %s %s' % (deviceid, self.subscription.pendingChanges))
                
                while deviceid in self.subscription.pendingChanges and count<maxcount:
                    await asyncio.sleep(.1)
                    count=count+1
                    if count>=maxcount:
                        self.log.info('!! Timeout waiting for pending changes on %s' % deviceid)
                return True
            except:
                self.log.error('!! Error during wait for pending change for %s (%s)' % (deviceid, count), exc_info=True)
                return True
                
        async def group_members(self, endpointId):
            try:
                groupmembers=[]
                #self.log.debug('.. getting group members for %s' % endpointId)
                for link in self.dataset.nativeDevices['group'][endpointId.split(':',2)[2]]['members']['link']:
                    groupmembers.append('insteon:node:%s' % link['#text'])
            except:
                self.log.error('!! error trying to get virtual group members: %s' % group_id, exc_info=True)
            return groupmembers 
               
        async def virtual_group_handler(self, controllers, devicelist):
            try:
                for group in self.dataset.nativeDevices['group']:
                    dev=self.dataset.getDeviceByEndpointId("insteon:group:%s" % group)
                    if dev:
                        all_controllers=True
                        for controller in controllers:
                            if not hasattr(dev, controller):
                                all_controllers=False
                        
                        if all_controllers:
                            groupmembers=await self.group_members("insteon:group:%s" % group)
                            if Counter(devicelist) == Counter(groupmembers):  
                                return { "id": "insteon:group:%s" % group, "name": self.dataset.nativeDevices['group'][group]['name'], "members": groupmembers }
                            
                #self.log.info('.. no virtual group found for %s' % devicelist)
            except:
                self.log.error('!! error trying to get virtual group: %s' % devicelist, exc_info=True)
                
            return {}
            
if __name__ == '__main__':
    adapter=insteon(name="insteon")
    adapter.start()