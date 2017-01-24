import datetime
import importlib
import inspect
import json
import os
import re
from collections import defaultdict
from pprint import pprint

import jsonpickle
import requests
from bson import json_util, ObjectId, DBRef
from requests.cookies import RequestsCookieJar

from app.common.core.context import TeamContext
from app.pipeline import FetchMetrics, FindMetricsForSources
from app.ingest.connectors.plug_connectors_python2 import PluggableInfo
from orm import Team, Source, Tag, TagRule, ObjectDefinition, Task, AccessToken
from app.common import services

TeamContext().team_id = 'XXXXXXXXXX'


class Api(object):
    def __init__(self, env):
        self.session = requests.session()
        self.local_data_file_name = os.path.dirname(os.path.abspath(__file__)) + '/local.json'
        self.env = env

        self.host = 'https://dash{env}.origamilogic.com'.format(env=self.env)

        self.local_data = self.load_local_data()

        if not self.local_data.get(env):
            raise LookupError("Can't find login information for %s environment" % env)

        self.login_data = {
            'email': self.local_data[env]['email'],
            'password': self.local_data[env]['password'],
            'next': '/api/Team'
        }

        self.headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.host,
            # "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
        }

        self.session.cookies = self.local_data.get('cookies', {}).get(self.host, RequestsCookieJar())

        if not self.check_session():
            self.authorize()

    def authorize(self):
        print 'authorizing'
        response = self.session.get(self.host)
        csrfmiddlewaretokens = re.findall(r"csrfmiddlewaretoken' value='(.*?)'", response.content)

        print csrfmiddlewaretokens

        self.session.post(
            self.host + '/login',
            data=dict(self.login_data, csrfmiddlewaretoken=csrfmiddlewaretokens[0]),
            headers=self.headers
        )

        self.local_data.setdefault('cookies', {})[self.host] = self.session.cookies

        self.save_local_data()

    def get(self, url, params):
        response = self.session.get(self.host + url, params=params)
        return response

    def check_session(self):
        response = self.session.get(self.host + '/api/Team')
        return response.content == 'null'

    def save_local_data(self):
        with open(self.local_data_file_name, 'w') as f:
            f.write(json.dumps(json.loads(jsonpickle.encode(self.local_data)), indent=4, sort_keys=False))

    def load_local_data(self):
        if os.path.isfile(self.local_data_file_name):
            with open(self.local_data_file_name) as f:
                return jsonpickle.decode(f.read())
        return {}

    def fetch_paged_data(self, entity, query):
        page = 1
        total_pages = None
        query = json_util.dumps(query)

        url = self.host + '/api/' + entity + '/staff_only_find_all_paged'

        while True:
            print 'Fetching', entity, 'page', page, 'out of', total_pages
            response = self.session.get(url+'?filter_query=$json:%s&page=%s' % (query, page))
            data = json_util.loads(response.content)

            # print data

            yield data['items']

            total_pages = data['total_pages']

            if page >= total_pages:
                break
            else:
                page += 1

    def sync_object_definitions(self):

        for items in self.fetch_paged_data('ObjectDefinition', {}):
            for doc in items:
                ObjectDefinition.save(doc)


    def get_source(self, source_id):
        access_tokens = set()

        for items in self.fetch_paged_data('Source', {'_id': ObjectId(source_id)}):
            for doc in items:
                access_tokens.update({x['access_token_id'] for x in doc.get('access_token', [])})
                Source.save(doc)
                source = doc

        for items in self.fetch_paged_data('AccessToken', {'_id': {"$in": [ObjectId(x) for x in access_tokens]}}):
            for doc in items:
                AccessToken.save(doc)

        return source

    def sync_team_data(self, team_id):
        # sync Team
        for items in self.fetch_paged_data('Team', {'_id': ObjectId(team_id)}):
            for doc in items:
                Team.save(doc)

        # sync Sources
        for items in self.fetch_paged_data('Source', {'team': DBRef('projects', ObjectId(team_id))}):
            for doc in items:
                Source.save(doc)

        # sync Tags
        for items in self.fetch_paged_data('Tag', {'team_id': team_id}):
            for doc in items:
                Tag.save(doc)

        # sync TagRule
        for items in self.fetch_paged_data('TagRule', {'team_id': team_id}):
            for doc in items:
                TagRule.save(doc)

    def run_task(self, task_id, data=None):
        data = data or self.get_task_args(task_id)

        data['arguments']['kwargs'].update({
            "service": data['arguments']['args'][0],
            "connector_type": data['arguments']['args'][1],
        })

        runs = [data['arguments']['kwargs']]

        metrics = []

        while len(runs):
            print len(runs)
            kw = runs.pop(0)

            python_mod = '%s.%s.%s' % ("app.ingest.connectors", kw['service'], kw['connector_type'])
            conn_mod = importlib.import_module(python_mod)

            fetcher = getattr(conn_mod, 'Fetcher', None)()
            parser = getattr(conn_mod, 'Parser', None)
            process = getattr(conn_mod, 'Process', None)

            if fetcher:
                function_kwargs = {x: kw[x] for x in inspect.getargspec(fetcher.get).args[1:]}

                try:
                    kw.update(fetcher.get(**function_kwargs))
                except BaseException as e:
                    raise
            if parser:
                parser = parser()
                kw.update(parser.parse(**{x: kw[x] for x in inspect.getargspec(parser.parse).args[1:]}))

            if kw.get('data'):
                metrics.extend(kw.get('data'))

            if process:
                process = process()
                results = None
                if hasattr(process, 'process'):
                    function_kwargs = {x: kw[x] for x in inspect.getargspec(process.process).args[1:]}
                    results = process.process(**function_kwargs)
                elif hasattr(process, 'addl_connector_run'):
                    function_kwargs = {x: kw[x] for x in inspect.getargspec(process.addl_connector_run).args[1:]}
                    results = process.addl_connector_run(**function_kwargs)
                if results:
                    for result in results:
                        runs.append(dict(kw, **result))

        return metrics

    def run_task2(self, task_id):
        try:
            with open(task_id+'.task') as f:
                print 'using locally saved task'
                data = json_util.loads(f.read())
        except:
            response = self.session.get(self.host + '/api/TaskArgs/' + task_id)

            with open(task_id+'.task', 'w') as f:
                f.write(response.content)

            print response.content

            data = json_util.loads(response.content)

        data['arguments'] = jsonpickle.decode(data['arguments'])

        args = data['arguments']['args']
        kwargs = data['arguments']['kwargs']

        kwargs['sync'] = True

        if kwargs.get('is_message_fetch_flow'):
            from app.ingest.framework.messages_connector_run import connector_run
            connector_run(args[0], args[1], **kwargs)
        else:
            from app.ingest.framework.connector_run import connector_run
            connector_run(data['arguments']['args'][0], data['arguments']['args'][1], **data['arguments']['kwargs'])

    def get_task_args(self, task_id):

        file_name = 'tasks/' + task_id+'.json'

        if os.path.isfile(file_name):
            # print 'using locally saved task', task_id+'.json'
            with open(file_name) as f:
                data = json_util.loads(f.read())
        else:
            response = self.session.get(self.host + '/api/TaskArgs/' + task_id)

            with open(file_name, 'w') as f:
                f.write(response.content)

            data = json_util.loads(response.content)

        data['arguments'] = jsonpickle.decode(data['arguments'])

        return data


    def find_parent_task(self, task_id):
        while True:
            response = self.session.get(self.host + '/api/Task/' + task_id)

            data = json_util.loads(response.content)
            ancestor_status_id = data.get('ancestor_status_id')
            print data['_id'], data['task_name'], ancestor_status_id, data['_started_at'], data.get('runtime_seconds')
            if ancestor_status_id:
                task_id = ancestor_status_id
            else:
                pprint(data)
                break

    def fetch_task_for_job_and_source(self, job_id, source_id):
        for items in api.fetch_paged_data('Task', {
            "jobrunref": {"$id": {"$oid": job_id}, "$ref": "job_status_details"},
            "base_queue_name": "fetch_bing_ads",
            "task_name": {"$regex": "sid=%s" % source_id}
        }):
            for doc in items:
                Task.save(doc)

    def generate_templates(self, team_id, service_id):

        MetricTemplate = services.Factory.create('MetricTemplate')
        for doc in PluggableInfo().template_generators[service_id].generate_templates():
            MetricTemplate.post(doc)

        templates = PluggableInfo().template_generators[service_id].generate_templates()
        MetricTemplate.bulk_post(team_id, templates)

        return PluggableInfo().template_generators[service_id].generate_mtemplates()

    def create_metric_instances(self, source, metric_templates):
        metric_service = services.Factory.create('Metric')

        if metric_templates is None:
            templates = self.generate_templates(source['team_id'], source['service_name'])

        filtered_templates = list(x for x in templates if all([
            x['metric_type'] == 'connector',
            x.get('from_messages') is None
        ]))

        for template in filtered_templates:

            allowed_dimensions = template['allowed_dimensions']

            if template.get('dimension_required') != True:
                allowed_dimensions.append(None)

            for dimension in allowed_dimensions:
                metric_service.find_or_create(**{
                    'team_id': source['team_id'],
                    'service': source['service_id'],
                    'metric_owner_object_id': source['primary_key'],
                    'name': template['name'],
                    'granularity': template['granularity'],
                    'dimensions': [dimension] if dimension else [],
                    'dim_filter': None,
                    'object_type': template.get('metric_owner_object_type'),
                    'create_dependencies': False
                })

    def get_messages(self, source_id):
        from app.ingest.framework import messages_connector_run

        source = Source.get_with_id(source_id)

        if source is None:
            source = self.get_source(source_id)

        service = services.Factory.create('Service').find_with_service_name(
            team_id=None, service_name=source['service_name'])

        for object_type, fetch_configuration in services.Factory.create('ObjectDefinition').fetch_configuration(
                object_type=service['object_type']).items():
            print object_type
            print fetch_configuration
            print source

            kwargs = {
                'sync': True,
                'queue': None,
                'start_date': '2000-01-01',
                'source_channel_id': source['primary_key'],
                'value_object_type': object_type,
                'fetch_data': {'parent_object': (service['object_type'], None)}
            }

            #ToDo: missing active configuration
            messages_connector_run.connector_run(*fetch_configuration['connector'], **kwargs)

    def get_templates_for_source(self):
        return []

if __name__ == "__main__":
    api = Api('-i1')

    pprint(api.get_task_args('5886088fe6dcb22ed9b0924a'))

    api.fetch_task_for_job_and_source("583a9280e4b097db41f1fd1f", "562aa503619c8c1fe4d7ecfa")
    api.find_parent_task('583bf76b1a7f4409788bf168')
