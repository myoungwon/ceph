"""
Run ceph-dedup-tool
"""
import contextlib
import logging
import gevent
from teuthology import misc as teuthology
import json
import time
from io import StringIO


from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run ceph-dedup-tool.
    The config should be as follows::
        ceph-dedup-tool:
          clients: [client list]
          op: <operation name>
          pool: <pool name>
          chunk_pool: <chunk pool name>
          chunk_size: <chunk size>
          chunk_algorithm: <chunk algorithm, fixed|fastcdc>
          fingerprint_algorithm: <fingerprint algorithm, sha1|sha256|sha512>
          chunk_dedup_threashold: <the number of duplicate chunks to trigger chunk dedup>
          max_thread: <the number of threads>
          wakeup_period: <duration>
    For example::
        tasks:
        - exec:
            client.0:
              - sudo ceph osd pool create low_tier 4
        - deduplication:
            clients: [client.0]
            op: 'sample-dedup'
            pool: 'default.rgw.buckets.data'
            chunk_pool: 'low_tier'
            chunk_size: 8192
            chunk_algorithm: 'fastcdc'
            fingerprint_algorithm: 'sha1'
            chunk_dedup_threshold: 5
            max_thread: 2
            wakeup_period: 20
            sampling_ratio: 100
    """
    log.info('Beginning deduplication...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    #assert hasattr(ctx, 'rgw')
    testdir = teuthology.get_testdir(ctx)
    args = [
        'ceph-dedup-tool']
    if config.get('op', None):
        args.extend(['--op', config.get('op', None)])
    if config.get('chunk_pool', None):
        args.extend(['--chunk-pool', config.get('chunk_pool', None)])
    if config.get('chunk_size', False):
        args.extend(['--chunk-size', str(config.get('chunk_size', 16384))])
    if config.get('chunk_algorithm', False):
        args.extend(['--chunk-algorithm', config.get('chunk_algorithm', None)] )
    if config.get('fingerprint_algorithm', False):
        args.extend(['--fingerprint-algorithm', config.get('fingerprint_algorithm', None)] )
    if config.get('chunk_dedup_threshold', False):
        args.extend(['--chunk-dedup-threshold', str(config.get('chunk_dedup_threshold', 1))])
    if config.get('max_thread', False):
        args.extend(['--max-thread', str(config.get('max_thread', 2))])
    if config.get('sampling_ratio', False):
        args.extend(['--sampling-ratio', str(config.get('sampling_ratio', 100))])
    if config.get('wakeup_period', False):
        args.extend(['--wakeup-period', str(config.get('wakeup_period', 20))])
    if config.get('pool', False):
        args.extend(['--pool', config.get('pool', None)])

    args.extend([
        '--debug',
        '--daemon',
        '--loop'])
        #'--debug'])

    def thread():
        run_remote(args, False)

    def run_remote(args, need_wait):
        clients = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        log.info('clients are %s' % clients)
        role = config.get('clients', clients)[0]
        assert isinstance(role, str)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        testdir = teuthology.get_testdir(ctx)
        cmd_args = [
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir)]
        cmd_args.extend(args)
        log.info("cmd: %s", cmd_args)
        (remote,) = ctx.cluster.only(role).remotes.keys()
        proc = remote.run(
            args=cmd_args,
            wait=need_wait, check_status=False,
            stdout=StringIO(),
            stderr=StringIO(),
            )
        log.info('proc exist ', proc.exitstatus, proc.stdout.getvalue(), proc.stderr.getvalue())
        if proc.exitstatus != 0:
            return False
        return proc.stdout.getvalue().strip()

    def get_chunk_objs(chunk_pool):
        chunk_obj_list = run_remote(('rados ls -p ' + chunk_pool).split(), True).split()
        if chunk_obj_list == False:
            return None 
        else:
            return chunk_obj_list

    # To validate whether the sample-dedup operation works well, this function checks if 
    #   1. sample-dedup has been started and
    #   2. reference of chunk objects' exist in correct base pool
    def validate():
        log.info('start validating sample-dedup')
        base_pool = config.get('pool', None)
        chunk_pool = config.get('chunk_pool', None)
        max_validation_cnt = 20
        retry_cnt = 0

        # check whether sample-dedup has been started
        chunk_obj_list = get_chunk_objs(chunk_pool)
        while chunk_obj_list == None and retry_cnt < max_validation_cnt:
            # retry getting # chunk objs after 30 secs of sleep
            time.sleep(30)
            chunk_obj_list = get_chunk_objs(chunk_pool)
            retry_cnt += 1
            log.info('chunk pool empty. retry ', retry_cnt)
        assert retry_cnt < max_validation_cnt

        log.info('sample-dedup started successfully')

        retry_cnt = 0
        # validate chunk pool for max_validation_cnt times
        while retry_cnt < max_validation_cnt:
            for chunk_obj in chunk_obj_list:
                # get reference list of chunk object
                dump_str = run_remote(
                    ('ceph-dedup-tool --op dump-chunk-refs --chunk-pool '
                    + chunk_pool + ' --object ' + chunk_obj).split(),
                    True
                )
                # fail in case that reference object is not written
                assert len(dump_str) > 0
                log.info('{0} obj has {1} refs'
                    .format(chunk_obj, json.loads(dump_str)['count']))

                # check if chunk object's reference object exists in base-tier
                ref_list = json.loads(dump_str)['refs']
                for ref in ref_list:
                    ret = run_remote(('rados -p ' + base_pool + ' stat ' + ref['oid']).split(), True)
                    # check if ref exists in base pool
                    assert len(ret)
                    log.info('{0} obj exists in {1}'.format(ref['oid'], base_pool))

            # get chunk objects for the next loop
            chunk_obj_list = get_chunk_objs(chunk_pool)
            retry_cnt += 1
            time.sleep(30)
        return True


    running = gevent.spawn(thread)
    checker = gevent.spawn(validate)

    try:
        yield
    finally:
        log.info('joining ceph-dedup-tool')
        running.get()
        checker.get()

