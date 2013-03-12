<?php

class runner {
	
	const File = "bin/slist.txt";
	const segments = 5;
	const instances = 30; //--- instances per 5segs --

	protected $command = "bin/ccRunExample AmazonEMR ExampleBackwards uniqhosts ";
	protected $slist = array();

	public function __construct() {

	}


	public function run() {
		foreach ( file(self::File) as $line ) {
			$line = trim($line);
			$this->makeCommand ( $line );
		}
	}

	protected function makeCommand ( $segmentId ) {
		$this->slist[] = $segmentId;
		if ( count ( $slist ) == self::segments ) {
			$string = implode ( ",", $this->slist ); //--- slist ---
			$this->coreCommand ( $string );
			$this->slist = array();
		}
	}

	protected function coreCommand ( $string ) {
		$tostart = $this->command." ".self::instances." ".$string;
		echo $tostart."\n";
	}
}

$r = new runner();
$r->run();