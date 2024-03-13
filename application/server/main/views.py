import redis
from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

import application.server.main.tasks as tasks

default_timeout = 43200000

main_blueprint = Blueprint(
    "main",
    __name__,
)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("home.html")


@main_blueprint.route("/harvest_compute", methods=["POST"])
def run_task_harvest_inpi():
    """
    Harvest of the INPI database.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_harvest_inpi, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/harvest_compute_split", methods=["POST"])
def run_task_harvest_inpi_split():
    """
    Harvest of the INPI database with split jobs.
    """
    args = request.get_json(force=True)

    # Download and unzip inpi db
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        download_task = q.enqueue(tasks.task_download_and_unzip_inpi, args)
    response_object = {"status": "success", "data": {"task_id": download_task.get_id()}}

    # Load mongo without history
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        no_history_task = q.enqueue(tasks.task_mongo_load, args, depends_on=[download_task])
    response_object = {"status": "success", "data": {"task_id": no_history_task.get_id()}}

    # Load mongo with history
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        history_task = q.enqueue(tasks.task_mongo_load_with_history, args, depends_on=[download_task])
    response_object = {"status": "success", "data": {"task_id": history_task.get_id()}}

    # Delete load file
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        delete_task = q.enqueue(tasks.task_remove_csv, args, depends_on=[no_history_task, history_task])
    response_object = {"status": "success", "data": {"task_id": delete_task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_load_force", methods=["POST"])
def run_task_mongo_load_force():
    """
    Forced load of the mongo db.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_load_force, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_load_force_with_history, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_reload", methods=["POST"])
def run_task_mongo_reload():
    """
    Reload of the mongo db.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload_with_history, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202

@main_blueprint.route("/mongo_reload_no_history", methods=["POST"])
def run_task_mongo_reload_no_history():
    """
    Reload of the mongo db for collections without history.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_reload_with_history", methods=["POST"])
def run_task_mongo_reload_with_history():
    """
    Reload of the mongo db for collections with history.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload_with_history, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_reload_force", methods=["POST"])
def run_task_mongo_reload_force():
    """
    Forced reload of the mongo db.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload_force, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload_force_with_history, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_load_force_no_history", methods=["POST"])
def run_task_mongo_load_force_no_history():
    """
    Forced load of the mongo db for collections without history.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_load_force, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_load_force_with_history", methods=["POST"])
def run_task_mongo_load_force_with_history():
    """
    Load of the mongo db for collections with history.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_load_force_with_history, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_reload_force_no_history", methods=["POST"])
def run_task_mongo_reload_force_no_history():
    """
    Forced reload of the mongo db for collections without history.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload_force, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_reload_force_with_history", methods=["POST"])
def run_task_mongo_reload_force_with_history():
    """
    Forced reload of the mongo db for collections with history.
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_reload_force_with_history, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/mongo_delete_duplicates", methods=["POST"])
def run_task_mongo_delete_duplicates():
    """
    Remove exact duplicates from mongo collections.
    """
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_mongo_delete_duplicates, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/clean", methods=["POST"])
def run_task_disk_clean():
    """
    Clean all data from disk.
    """
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="inpi", default_timeout=default_timeout)
        task = q.enqueue(tasks.task_disk_clean, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202
