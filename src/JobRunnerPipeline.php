<?php

use Google\Cloud\SecretManager\V1\SecretManagerServiceClient;

class JobRunnerPipeline {
	/** @var array[CurlMultiHandle] */
	protected $curlMultiHandle = [];
	/** @var string */
	protected $secretKey;
	/** @var array[int] */
	protected $slotCount = [];
	/** @var RedisJobService */
	protected $srvc;

	protected function wglFetchSecret( $secretName ) {
		$client = new SecretManagerServiceClient();
		$name = $client->secretVersionName( $this->srvc->project, 'mediawiki-' . $secretName, 'latest');
		$response = $client->accessSecretVersion($name);
		return $response->getPayload()->getData();
	}

	/**
	 * @param RedisJobService $service
	 */
	public function __construct( RedisJobService $service ) {
		$this->srvc = $service;
		$this->secretKey = $this->wglFetchSecret( 'wgSecretKey' );

		foreach( $this->srvc->loopMap as $loop => $info ) {
			$this->slotCount[$loop] = 0;
			$this->curlMultiHandle[$loop] = curl_multi_init();

			// Maximum number of cURL connections to keep open.
			curl_multi_setopt( $this->curlMultiHandle[$loop], CURLMOPT_MAXCONNECTS, $info['runners'] );
			curl_multi_setopt( $this->curlMultiHandle[$loop], CURLMOPT_MAX_HOST_CONNECTIONS, $info['runners'] );
			curl_multi_setopt( $this->curlMultiHandle[$loop], CURLMOPT_MAX_TOTAL_CONNECTIONS, $info['runners'] );
		}
	}

	/**
	 * @param integer $loop
	 * @param array $prioMap
	 * @param array $pending
	 * @return array
	 */
	public function refillSlots( int $loop, array $prioMap, array &$pending ) : array {
		$maxSlots = $this->srvc->loopMap[$loop]['runners'];
		$new = 0;
		$host = gethostname();
		$cTime = time();

		curl_multi_exec( $this->curlMultiHandle[$loop], $active );
		while ( false !== ( $info = curl_multi_info_read( $this->curlMultiHandle[$loop] ) ) ) {
			if ( $info['msg'] === CURLMSG_DONE ) {
				$slotData = json_decode( curl_getinfo( $info['handle'], CURLINFO_PRIVATE ) );
				$httpCode = curl_getinfo( $info['handle'], CURLINFO_HTTP_CODE );
				// $result will be an array if no exceptions happened.
				$content = curl_multi_getcontent( $info['handle'] );
				$result = json_decode( trim( $content ), true );
				if ( is_array( $result ) ) {
					// If this finished early, lay off of the queue for a while
					if ( ( $cTime - $slotData['stime'] ) < $this->srvc->hpMaxTime / 2 ) {
						unset( $pending[$slotData['type']][$slotData['db']] );
						$this->srvc->debug( "Queue '{$slotData['db']}/{$slotData['type']}' emptied." );
					}
					$ok = 0; // jobs that ran OK
					foreach ( $result['jobs'] as $status ) {
						$ok += ( $status['status'] === 'ok' ) ? 1 : 0;
					}
					$failed = count( $result['jobs'] ) - $ok;
					$this->srvc->incrStats( "pop.{$slotData['type']}.ok.{$host}", $ok );
					$this->srvc->incrStats( "pop.{$slotData['type']}.failed.{$host}", $failed );
				} else {
					$error = $content;
					// Mention any serious errors that may have occured
					if ( strlen( $error ) > 4096 ) { // truncate long errors
						$error = mb_substr( $error, 0, 4096 ) . '...';
					}
					$this->srvc->error( "Runner loop $loop process gave status '{$httpCode}' ({$slotData['type']}, {$slotData['db']}):\n\t$error" );
					$this->srvc->incrStats( 'runner-status.error' );
				}
				curl_multi_remove_handle( $this->curlMultiHandle[$loop], $info['handle'] );
				curl_close( $info['handle'] );
				$this->slotCount--;
			}
		}

		$queue = $this->selectQueue( $loop, $prioMap, $pending );
		// Make sure this is a known wiki in the queue.
		if ( $queue && isset($this->srvc->wikis[$queue[1]]) && $this->slotCount < $maxSlots ) {
			// Spawn a job runner for this loop ID.
			$highPrio = $prioMap[$loop]['high'];
			$this->spawnRunner( $loop, $highPrio, $queue );
			++$new;
		}

		return [ ( $maxSlots - $this->slotCount ), $new ];
	}

	/**
	 * @param integer $loop
	 * @param array $prioMap
	 * @param array $pending
	 * @return array|boolean
	 */
	protected function selectQueue( int $loop, array $prioMap, array $pending ) {
		$include = $this->srvc->loopMap[$loop]['include'];
		$exclude = $this->srvc->loopMap[$loop]['exclude'];
		if ( $prioMap[$loop]['high'] ) {
			$exclude = array_merge( $exclude, $this->srvc->loopMap[$loop]['low-priority'] );
		} else {
			$include = array_merge( $include, $this->srvc->loopMap[$loop]['low-priority'] );
		}
		if ( in_array( '*', $include ) ) {
			$include = array_merge( $include, array_keys( $pending ) );
		}

		$candidateTypes = array_diff( array_unique( $include ), $exclude, [ '*' ] );

		$candidates = []; // list of (type, db)
		// Flatten the tree of candidates into a flat list so that a random
		// item can be selected, weighing each queue (type/db tuple) equally.
		foreach ( $candidateTypes as $type ) {
			if ( isset( $pending[$type] ) ) {
				foreach ( $pending[$type] as $db => $since ) {
					$candidates[] = [ $type, $db ];
				}
			}
		}

		if ( !count( $candidates ) ) {
			return false; // no jobs for this type
		}

		return $candidates[mt_rand( 0, count( $candidates ) - 1 )];
	}

	/**
	 * @param integer $loop
	 * @param bool $highPrio
	 * @param array $queue
	 */
	protected function spawnRunner( int $loop, bool $highPrio, array $queue ) : bool {
		$this->srvc->debug( "Spawning runner in loop $loop ($type, $db)." );

		// Pick a random queue.
		list( $type, $db ) = $queue;
		$maxtime = $highPrio ? $this->srvc->lpMaxTime : $this->srvc->hpMaxTime;
		$host = $this->srvc->wikis[$db];
		$url = $this->srvc->url;

		$postfields = 'async=false&maxtime=' . rawurlencode($maxtime) . '&sigexpiry=2147483647&tasks=placeholder&title=Special:RunJobs&type=' . rawurlencode($type);
		$signature = hash_hmac( 'sha1', $postfields, $this->secretKey );
		$postfields .= "&signature=$signature";

		// Prepare the runner.
		$ch = curl_init();
		curl_setopt_array( $ch, [
			CURLOPT_CONNECTTIMEOUT => 5,
			CURLOPT_HEADER => false,
			CURLOPT_HTTPHEADER => [ 'Host' => $host ],
			CURLOPT_POST => true,
			CURLOPT_POSTFIELDS => $postfields,
			CURLOPT_PRIVATE => json_encode([
				'db' => $db,
				'stime' => time(),
				'type' => $type,
			]),
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_TCP_KEEPALIVE => 1,
			CURLOPT_TCP_NODELAY => true,
			CURLOPT_TIMEOUT => $maxtime + 5,
			CURLOPT_URL => $url,
		]);
		curl_multi_add_handle( $this->curlMultiHandle[$loop], $ch );
	}
}
