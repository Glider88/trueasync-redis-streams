<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams;

use Closure;
use Glider88\AsyncRedisStreams\Helpers\Php;
use Glider88\AsyncRedisStreams\Helpers\Str;
use Glider88\AsyncRedisStreams\Helpers\Time;
use function Async\{await, delay, spawn};

require  __DIR__ . '/bootstrap.php';
require  __DIR__ . '/../vendor/autoload.php';


$startMs = Time::nowMs();
$i = 1;
$f = static function () use (&$i, &$startMs, &$mem, &$time) {
    $nowMs = Time::nowMs();
    if (($nowMs - $startMs) >= 1000) {
        Php::mem(__LINE__);
        Str::println("rpc: $i");

        $i = 1;
        $startMs = $nowMs;
    }

    $i += 1;
};

$handler = new readonly class($f) implements MessageHandlerInterface
{
    public function __construct(
        private Closure $run
    ) {}

    public function handle(array $message): void
    {
        $c = spawn(function () {
            delay(100);
        });
        await($c);

        $this->run->__invoke($message);
    }
};

$config = mkConfig();
$stream = new Stream($config);

$stream->run($handler);
