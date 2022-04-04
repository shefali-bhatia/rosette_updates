# prepare rosette benchmarks for the purpose of testing rosette solver
from curses import raw
import sched
import time
import os
import json
import csv
from subprocess import Popen, PIPE, STDOUT, check_output
import time
import signal

from pprint import pprint

from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

import sys

str_cols_dict = {}

def run_parser(sql_file):
    print("[INFO] Send SQL file into parser")
    parser_cmd = f"java -classpath /Users/mencher/Projects/tmp/cosette-parser/target/classes:/Users/mencher/.m2/repository/org/apache/calcite/calcite-core/1.27.0/calcite-core-1.27.0.jar:/Users/mencher/.m2/repository/org/apache/calcite/calcite-linq4j/1.27.0/calcite-linq4j-1.27.0.jar:/Users/mencher/.m2/repository/com/esri/geometry/esri-geometry-api/2.2.0/esri-geometry-api-2.2.0.jar:/Users/mencher/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.10.0/jackson-annotations-2.10.0.jar:/Users/mencher/.m2/repository/com/google/errorprone/error_prone_annotations/2.5.1/error_prone_annotations-2.5.1.jar:/Users/mencher/.m2/repository/com/google/guava/guava/29.0-jre/guava-29.0-jre.jar:/Users/mencher/.m2/repository/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:/Users/mencher/.m2/repository/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/Users/mencher/.m2/repository/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:/Users/mencher/.m2/repository/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar:/Users/mencher/.m2/repository/org/apiguardian/apiguardian-api/1.1.0/apiguardian-api-1.1.0.jar:/Users/mencher/.m2/repository/org/checkerframework/checker-qual/3.10.0/checker-qual-3.10.0.jar:/Users/mencher/.m2/repository/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:/Users/mencher/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.10.0/jackson-core-2.10.0.jar:/Users/mencher/.m2/repository/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/2.10.0/jackson-dataformat-yaml-2.10.0.jar:/Users/mencher/.m2/repository/org/yaml/snakeyaml/1.24/snakeyaml-1.24.jar:/Users/mencher/.m2/repository/com/google/uzaygezen/uzaygezen-core/0.2/uzaygezen-core-0.2.jar:/Users/mencher/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar:/Users/mencher/.m2/repository/com/jayway/jsonpath/json-path/2.4.0/json-path-2.4.0.jar:/Users/mencher/.m2/repository/net/minidev/json-smart/2.3/json-smart-2.3.jar:/Users/mencher/.m2/repository/net/minidev/accessors-smart/1.2/accessors-smart-1.2.jar:/Users/mencher/.m2/repository/org/ow2/asm/asm/5.0.4/asm-5.0.4.jar:/Users/mencher/.m2/repository/com/yahoo/datasketches/sketches-core/0.9.0/sketches-core-0.9.0.jar:/Users/mencher/.m2/repository/com/yahoo/datasketches/memory/0.9.0/memory-0.9.0.jar:/Users/mencher/.m2/repository/commons-codec/commons-codec/1.13/commons-codec-1.13.jar:/Users/mencher/.m2/repository/net/hydromatic/aggdesigner-algorithm/6.0/aggdesigner-algorithm-6.0.jar:/Users/mencher/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar:/Users/mencher/.m2/repository/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar:/Users/mencher/.m2/repository/org/apache/commons/commons-dbcp2/2.6.0/commons-dbcp2-2.6.0.jar:/Users/mencher/.m2/repository/org/apache/commons/commons-pool2/2.6.1/commons-pool2-2.6.1.jar:/Users/mencher/.m2/repository/org/apache/commons/commons-lang3/3.8/commons-lang3-3.8.jar:/Users/mencher/.m2/repository/org/codehaus/janino/commons-compiler/3.0.11/commons-compiler-3.0.11.jar:/Users/mencher/.m2/repository/org/codehaus/janino/janino/3.0.11/janino-3.0.11.jar:/Users/mencher/.m2/repository/org/apache/calcite/calcite-server/1.27.0/calcite-server-1.27.0.jar:/Users/mencher/.m2/repository/org/apache/calcite/avatica/avatica-core/1.18.0/avatica-core-1.18.0.jar:/Users/mencher/.m2/repository/org/apache/calcite/avatica/avatica-metrics/1.18.0/avatica-metrics-1.18.0.jar:/Users/mencher/.m2/repository/com/google/protobuf/protobuf-java/3.6.1/protobuf-java-3.6.1.jar:/Users/mencher/.m2/repository/org/apache/httpcomponents/httpclient/4.5.9/httpclient-4.5.9.jar:/Users/mencher/.m2/repository/org/apache/httpcomponents/httpcore/4.4.11/httpcore-4.4.11.jar:/Users/mencher/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.10.0/jackson-databind-2.10.0.jar:/Users/mencher/.m2/repository/commons-io/commons-io/2.7/commons-io-2.7.jar:/Users/mencher/.m2/repository/org/slf4j/slf4j-nop/1.7.25/slf4j-nop-1.7.25.jar org.cosette.Main {sql_file}"
    proc = Popen(parser_cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = proc.communicate()
    print("[INFO] Finish SQL parsing")
    str_stdout = str(stdout)
    if str_stdout.find("[FOR-PARSER]") != -1:
        if str_stdout.find("[FOR-PARSER] STRING-COL") != -1:
            extract_str = str_stdout[str_stdout.find("[FOR-PARSER] STRING-COL") + 24: str_stdout.find("[END-FOR-PARSER]") - 1]
            for pair in extract_str.split(" "):
                table_name, col_name = pair.split(".")[0], pair.split(".")[1]
                if table_name in str_cols_dict:
                    str_cols_dict[table_name].append(col_name)
                else:
                    str_cols_dict[table_name] = [ col_name ]
    # print(stdout, stderr)

def terminate_process(pid):
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except OSError as ex:
        print(f"[INFO] Process {pid} has been killed with following msg: {ex}")

# TODO: Probably we misunderstand the output if more than two counter example
# e.g. (((1 0 0) . 1)) (((3 2 1) . 2))) => (((1 0 0) . 1)) ((3 2 1) . 2)))
# If the later output is correct, we should write some codes to fix
def convert_output(trim_str, counters):
    idx_start, idx_end = trim_str.find("((") + 2, trim_str.find(")")
    split_data = trim_str[idx_start: idx_end].split(" ")
    for counter in counters: split_data[counter] = "\"" + split_data[counter] + "\""
    counters = []
    return trim_str[:idx_start] + " ".join(split_data) + trim_str[idx_end:]

def convert_string_col_to_question_mark(raw_output):
    # print(raw_output)
    # raw_output: (Table "INDIV_SAMPLE_NYC" '("CMTE_ID" "TRANSACTION_AMT" "NAME") '(((1 0 0) . 1)))
    # count = 0
    counters = []
    for target_table, target_cols in str_cols_dict.items():
        if raw_output.find(target_table) != -1:
            # col_names: "CMTE_ID" "TRANSACTION_AMT" "NAME"
            col_names = raw_output[raw_output.find("(", raw_output.find(target_table)) + 1: raw_output.find(")", raw_output.find(target_table))]
            split_col_names = col_names.split(" ")
            for target_col in target_cols:
                target_col = "\"" + target_col + "\""
                for i in range(len(split_col_names)):
                    if split_col_names[i] == target_col: counters.append(i)
            # if target_cols == ["TRANSACTION_AMT", "NAME"] => Counter = [ 1, 2 ]

            # Dealing with raw_output: (Table "INDIV_SAMPLE_NYC" '("CMTE_ID" "TRANSACTION_AMT" "NAME") '(((1 0 0) . 1)) (((3 2 1) . 2)))
            # which is we have multiple records
            # our goal is to convert them into "(((1 0 ?) . 1)) (((3 2 ?) . 2)))"
            idx_start = raw_output.find("(((", raw_output.find(target_table)) + 1
            idx_end = raw_output.find(")))", idx_start) + 3

            num_brace, last_start = 0, idx_start
            for idx in range(idx_start, idx_end):
                if raw_output[idx] == "(": num_brace = num_brace + 1
                elif raw_output[idx] == ")":
                   num_brace = num_brace - 1
                   if num_brace == 0:
                       converted_data = convert_output(raw_output[last_start:idx+1], counters)
                       raw_output = raw_output[:last_start] + converted_data + raw_output[idx+1:]
                       last_start = idx + 2
    return raw_output            


def run_equiv_check(rosette_file, rosette_dir, cosette_file, cosette_dir, time_limit, log_file=None):
    """ Run counter example search on the given rosette file
    Args:
        rosette_file: a rosette file that provides
    """

    ts = time.time()
    submission_file_name = str(ts)[:10]
    mv_cosette_file = f"../cosette-rs/tests/{submission_file_name}.json"
    mv_rosette_file = f"../rosette/tests/{submission_file_name}.rkt"
    
    new_cosette_file = f"./tests/{submission_file_name}.json"
    new_rosette_file = f"./tests/{submission_file_name}.rkt"
    ## Copy JSON file to Cosette-rs dir first
    cmd_mv_file = f"mv {cosette_file} {mv_cosette_file}; mv {rosette_file} {mv_rosette_file}"
    proc = Popen(cmd_mv_file, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = proc.communicate()

    # print(cmd_mv_file)

    cmd_cos = f'cd {cosette_dir}; cargo +nightly run --release -- {new_cosette_file}'
    cmd_ros = f'cd {rosette_dir}; racket {new_rosette_file}'

    # print(cmd_cos)
    # print(cmd_ros)

    if log_file:
        cmd_ros += " > {}".format(log_file)

    print("[INFO] Process Cosette & Rosette checking")
    cos_proc = Popen(cmd_cos, shell=True, stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid)
    ros_proc = Popen(cmd_ros, shell=True, stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid)


    #TODO: Since Cosette will return panic, we cannot terminate if received panic
    #Timer
    raw_output = ""
    # 0: No result, 1: Prove/No Counterexample, 2: Not Prove/Counterexample
    cos_result_status, ros_result_status = 0, 0
    while True:
        retcode_cos = cos_proc.poll()
        retcode_ros = ros_proc.poll()
        if cos_result_status == 0 and retcode_cos is not None:
            raw_output = cos_proc.stdout.read()
            print(cos_proc.stderr.read())
            raw_output = raw_output.decode("utf-8")
            if raw_output.find("provable") != -1:
                cos_result_status = 1
                print("[INFO] Kill Rosette process due to Rust's code prove two queries are equivalent")
                terminate_process(ros_proc.pid)
                break # Break while loop
            else:
                cos_result_status = 2
        elif ros_result_status == 0 and retcode_ros is not None:
            raw_output = ros_proc.stdout.read() + ros_proc.stderr.read()
            raw_output = raw_output.decode("utf-8")
            if raw_output.find("unsat") == -1:
                ros_result_status = 2
                print("[INFO] Kill Rust's process due to Rosette has returned counterexample")
                terminate_process(cos_proc.pid)
                break
            else:
                ros_result_status = 1
        elif time_limit <= 0:
            print("[INFO] Processes are killed due ot timeout")
            terminate_process(ros_proc.pid)
            terminate_process(cos_proc.pid)
            break
        elif cos_result_status == 2 and ros_result_status == 1:
            # Undecidable result
            break
        else:
            time_limit = time_limit - .1
            time.sleep(.1)
            continue

    print("[INFO] Delete temp output file")
    cmd_rm_file = f"rm {mv_cosette_file}; rm {mv_rosette_file}"
    proc = Popen(cmd_rm_file, shell=True, stdout=PIPE, stderr=PIPE)
    proc.communicate()

    # raw_output = raw_output.decode("utf-8")
    if ros_result_status == 2:
        print(raw_output)
        if raw_output.find("given") != -1:
            parsed_output = raw_output[raw_output.find("given") + 8: raw_output.find("context") - 4]
            if raw_output.find("LIKE regex does not match") != -1:
                return json.dumps({"status": "Undecidable", "test_log": parsed_output})
            else:
                return json.dumps({"status": "NEQ", "test_log": parsed_output})
        else:
            # Find whether we should replace the number to "?" for string columns
            parsed_output = convert_string_col_to_question_mark(raw_output)
            return json.dumps({"status": "NEQ", "test_log": parsed_output})
    elif cos_result_status == 1 and ros_result_status == 1:
        return json.dumps({"status": "EQ", "test_log": "SQL has been proven equivalent by Cosette"})
    elif cos_result_status == 2 and ros_result_status == 1:
        return json.dumps({"status": "Undecidable", "test_log": "Unable to prove two queries are equivalent and No counterexample has been found"})
    else:
        return json.dumps({"status": "ERROR", "test_log": "Processes have been terminated due to timeout"})

if __name__ == '__main__':
    # User Input
    file_name = "test_custom"

    test_dir = "tests"
    # # Step 1: Parse SQL into JSON & Racket format
    # input_sql = f"./{test_dir}/{file_name}.sql"
    # run_parser(input_sql)

    # # Step 2: Run Cosette & Rosette Equivalent Checking
    # parsed_json = f"./{test_dir}/{file_name}.json"
    # parsed_rkt = f"./{test_dir}/{file_name}.rkt"
    # result = run_equiv_check(parsed_rkt, "../rosette", parsed_json, "../cosette-rs", 10)
    # print(result)

@app.route("/check", methods=["POST"])
@cross_origin()
def check():
    global str_cols_dict
    data = request.get_json()

    required_params = ["schema", "ta_input", "student_input"]
    for param in required_params:
        if param not in data:
            return { "status": "ERROR", "msg": f"Missing required param: {param}"}

    # File define
    file_name = "test_custom"
    test_dir = "tests"
    input_sql = f"./{test_dir}/{file_name}.sql"

    f = open(input_sql, "w")
    f.write(data["schema"] + "\n\n")
    f.write(data["ta_input"] + "\n\n")
    f.write(data["student_input"])
    f.close()

    print(data["schema"])

    # Step 1: Parse SQL into JSON & Racket format
    run_parser(input_sql)

    # Step 2: Run Cosette & Rosette Equivalent Checking
    parsed_json = f"./{test_dir}/{file_name}.json"
    parsed_rkt = f"./{test_dir}/{file_name}.rkt"
    result = run_equiv_check(parsed_rkt, "../rosette", parsed_json, "../cosette-rs", 10)

    str_cols_dict = {}

    return result

app.run()