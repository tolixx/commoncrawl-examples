<?php

class runner {
	
	const File = "bin/slist.txt";
	const segments = 5;
	const instances = 30; //--- instances per 5segs --

	protected $command = "bin/ccRunExample AmazonEMR ExampleBackwards uniqhosts ";
	protected $slist = array();

	protected $realCommands = 0;

	public function __construct() {

	}

	public function __destruct() {
		echo "Runner : ".$this->realCommands. " commands ready.\n";
	}


	public function run() {
		foreach ( file(self::File) as $line ) {
			$line = trim($line);
			$this->makeCommand ( $line );
		}
		$this->makeCommand ( null, true );
	}

	protected function makeCommand ( $segmentId = null, $rest = false ) {
		if ( !is_null ( $segmentId ) ) {
		$this->slist[] = $segmentId;
	}
		if ( $rest || ( count ( $this->slist ) == self::segments ) ) {
			$string = implode ( ",", $this->slist ); //--- slist ---
			$this->coreCommand ( $string );
			$this->slist = array();
		}
	}

	protected function coreCommand ( $string ) {
		$tostart = $this->command." ".self::instances." ".$string;
		++$this->realCommands;
		echo $tostart."\n";
	}
}

$r = new runner();
$r->run();