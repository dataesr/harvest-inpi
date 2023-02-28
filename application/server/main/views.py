import json
import os
import pandas
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from application.server.main.tasks import create_task_all, create_task_clean

default_timeout = 43200000

main_blueprint = Blueprint('main', __name__, )



@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/harvest_compute', methods=['POST'])
def run_task_all():
    """
    All processes for patents
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='inpi', default_timeout=default_timeout)
        task = q.enqueue(create_task_all, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/clean', methods=['POST'])
def run_task_clean():
    """
    All processes for patents
    """
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='inpi', default_timeout=default_timeout)
        task = q.enqueue(create_task_clean, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202


