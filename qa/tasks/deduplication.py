"""
Run ceph-dedup-tool
"""
import contextlib
import logging
import gevent
from teuthology import misc as teuthology


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
          object: <object name>
          max_in_flight: <max number of operations in flight>
          object_size: <size of objects in bytes>
          min_stride_size: <minimum write stride size in bytes>
          max_stride_size: <maximum write stride size in bytes>
          op_weights: <dictionary mapping operation type to integer weight>
          runs: <number of times to run> - the pool is remade between runs
          ec_pool: use an ec pool
          erasure_code_profile: profile to use with the erasure coded pool
          fast_read: enable ec_pool's fast_read
          min_size: set the min_size of created pool
          pool_snaps: use pool snapshots instead of selfmanaged snapshots
	  write_fadvise_dontneed: write behavior like with LIBRADOS_OP_FLAG_FADVISE_DONTNEED.
	                          This mean data don't access in the near future.
				  Let osd backend don't keep data in cache.

    For example::

        tasks:
        - ceph:
        - rados:
            clients: [client.0]
            ops: 1000
            max_seconds: 0   # 0 for no limit
            objects: 25
            max_in_flight: 16
            object_size: 4000000
            min_stride_size: 1024
            max_stride_size: 4096
            op_weights:
              read: 20
              write: 10
              delete: 2
              snap_create: 3
              rollback: 2
              snap_remove: 0
            ec_pool: create an ec pool, defaults to False
            erasure_code_use_overwrites: test overwrites, default false
            erasure_code_profile:
              name: teuthologyprofile
              k: 2
              m: 1
              crush-failure-domain: osd
            pool_snaps: true
	    write_fadvise_dontneed: true
            runs: 10
        - interactive:

    Optionally, you can provide the pool name to run against:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo
        - rados:
            clients: [client.0]
            pools: [foo]
            ...

    """
    log.info('Beginning deduplication...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    #assert hasattr(ctx, 'rgw')
    testdir = teuthology.get_testdir(ctx)
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'ceph-dedup-tool']
    if config.get('op', None):
        args.extend(['--op', config.get('op', None)])
    if config.get('chunk_pool', None):
        args.extend(['--chunk-pool', config.get('chunk_pool', None)])
    if config.get('chunk_size', False):
        args.extend(['--chunk-size', str(config.get('chunk_size', 8192))])
    if config.get('chunk_algorithm', False):
        args.extend(['--chunk-algorithm', config.get('chunk_algorithm', None)] )
    if config.get('fingerprint_algorithm', False):
        args.extend(['--fingerprint-algorithm', config.get('fingerprint_algorithm', None)] )
    if config.get('object_dedup_threshold', False):
        args.extend(['--object-dedup-threshold', str(config.get('object_dedup_threshold', 50))])
    if config.get('chunk_dedup_threshold', False):
        args.extend(['--chunk-dedup-threshold', str(config.get('chunk_dedup_threshold', 5))])
    if config.get('max_thread', False):
        args.extend(['--max-thread', str(config.get('max_thread', 2))])
    if config.get('wakeup_period', False):
        args.extend(['"--wakeup-period"', str(config.get('wakeup_period', 30))])
    if config.get('pool', False):
        args.extend(['--pool', config.get('pool', None)])

    args.extend([
        '--debug',
        '--deamon',
        '--iterative'])

    def thread():
        clients = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        log.info('clients are %s' % clients)
        manager = ctx.managers['ceph']
        tests = {}
        log.info("args %s", args)
        for role in config.get('clients', clients):
            assert isinstance(role, str)
            PREFIX = 'client.'
            assert role.startswith(PREFIX)
            id_ = role[len(PREFIX):]
            (remote,) = ctx.cluster.only(role).remotes.keys()
            proc = remote.run(
                args=args,
                stdin=run.PIPE,
                wait=False
                )
            tests[id_] = proc

    running = gevent.spawn(thread)

    try:
        yield
    finally:
        log.info('joining ceph-dedup-tool')
        running.get()
