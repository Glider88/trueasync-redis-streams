<?php declare(strict_types=1);

namespace Tests\Glider88\AsyncRedisStreams;

use Exception;
use Glider88\AsyncRedisStreams\Backpressure\ConstantScaling;
use Glider88\AsyncRedisStreams\Config;
use Glider88\AsyncRedisStreams\Helpers\Arr;
use Glider88\AsyncRedisStreams\Helpers\Fun;
use Glider88\AsyncRedisStreams\Helpers\Time;
use Glider88\AsyncRedisStreams\Logger\ConsoleLogger;
use Glider88\AsyncRedisStreams\Logger\NullLogger;
use Glider88\AsyncRedisStreams\Redis\ClientInterface;
use Glider88\AsyncRedisStreams\Redis\PredisClientFactory;
use Glider88\AsyncRedisStreams\Retry\ConstantRetry;
use Glider88\AsyncRedisStreams\Stream;
use Glider88\AsyncRedisStreams\TimeInterval\Milli;
use Glider88\AsyncRedisStreams\TimeInterval\Sec;
use PHPUnit\Framework\TestCase;
use Tests\Glider88\AsyncRedisStreams\Utils\TestHandler;
use Tests\Glider88\AsyncRedisStreams\Utils\TestScaling;
use function Async\await_all;
use function Async\spawn;
use function Async\delay;

class StreamTest extends TestCase
{
    public function testSimplePath(): void
    {
        $f = function (array $msg) {
            $this->assertEquals(['one'], $msg);
        };

        $stream = new Stream($this->mkConfig());
        $stream->push('m1', ['one']);
        $stream->run(new TestHandler($f), 1);

        $stream->close();
    }

    public function testOverflow(): void
    {
        $cfg = $this->mkConfig(['maxStreamLength' => 1]);
        $stream = new Stream($cfg);
        foreach (range(1, 200) as $i) {
            $stream->push("m$i", [$i]);
        }

        $len = $this->mkRedis()->execute(['XLEN', 's']);
        $this->assertTrue($len < 200);

        $stream->close();
    }

    public function testManyGroups(): void
    {
        $cfg1 = $this->mkConfig(['group' => 'g1']);
        $cfg2 = $this->mkConfig(['group' => 'g2']);
        $stream1 = new Stream($cfg1);
        $stream2 = new Stream($cfg2);

        $stream1->push("1", ['one']);
        $stream1->push("2", ['two']);
        $stream1->push("3", ['three']);
        $stream1->push("4", ['four']);
        $stream1->push("5", ['five']);

        $h1 = new TestHandler(Fun::void());
        $h2 = new TestHandler(Fun::void());

        await_all([
            spawn(static fn() => $stream1->run($h1, 2)),
            spawn(static fn() => $stream2->run($h2, 3))
        ]);

        $r1 = Arr::flatten($h1->results);
        $r2 = Arr::flatten($h2->results);

        $this->assertEquals(['one', 'two'], $r1);
        $this->assertEquals(['one', 'two', 'three'], $r2);

        $stream1->close();
        $stream2->close();
    }

    public function testManyConsumers(): void
    {
        $oneWorker = new ConstantScaling(1);
        $cfg1 = $this->mkConfig(['consumer' => 'c1', 'scaling' => $oneWorker]);
        $cfg2 = $this->mkConfig(['consumer' => 'c2', 'scaling' => $oneWorker]);

        $stream1 = new Stream($cfg1);
        $stream2 = new Stream($cfg2);

        foreach (range(1, 9) as $i) {
            $stream1->push("m$i", [$i]);
        }

        $h1 = new TestHandler(Fun::void());
        $h2 = new TestHandler(Fun::void());

        await_all([
            spawn(static fn() => $stream1->run($h1, 4)),
            spawn(static fn() => $stream2->run($h2, 4))
        ]);

        $r1 = array_map(intval(...), Arr::flatten($h1->results));
        $r2 = array_map(intval(...), Arr::flatten($h2->results));

        $this->assertCount(4, $r1);
        $this->assertCount(4, $r2);

        $all = Arr::flatten([$r1, $r2]);
        sort($all);
        $this->assertEquals(range(1, 8), $all);

        $this->assertNotSame([1, 2, 3, 4], $r1);
        $this->assertNotSame([5, 6, 7, 8], $r2);

        $stream1->close();
        $stream2->close();
    }

    public function testRetryTask(): void
    {
        $props = [
            'stream' => 's',
            'maxRetries' => 3,
            'blockRead' => new Milli(100),
            'autoClaimMinIdle' => new Milli(1),
            'deduplicationTtl' => new Sec(5),
            'retryInterval' => new Milli(10),
            'claimInterval' => new Milli(10),
        ];

        $stream = new Stream($this->mkConfig($props));

        $predis = $this->mkRedis();
        $predis->execute(['XGROUP', 'CREATE', 's', 'g2', '$']);

        $stream->push('m1', ['one']);
        $r1 = $this->xReadGroup('g2', 'c', 10, 100, 's', '>');
        foreach ($r1['s'] as $item) {
            $this->assertEquals(0, (int) $item['_service_data_message_retries']);
        }

        $f = static function (array $msg) {
            throw new Exception('COROUTINE FAILED');
        };

        $stream->run(new TestHandler($f), 3);

        $r2 = $this->xReadGroup('g2', 'c', 10, 100, 's', '>');

        $rs = [];
        foreach ($r2['s'] as $item) {
            $rs[] = (int) $item['_service_data_message_retries'];
        }

        $this->assertEquals([1, 2], $rs);

        $stream->close();
    }

    public function testDeduplication(): void
    {
        $cfg = $this->mkConfig();
        $stream = new Stream($cfg);

        $stream->push('m1', ['one']);
        $stream->push('m1', ['two']);
        $stream->push('m2', ['three']);

        $h = new TestHandler(Fun::void());
        $stream->run($h, 2);

        $this->assertEquals([['one'], ['three']], $h->results);

        $stream->close();
    }

    public function testConcurrency(): void
    {
        $cfg = $this->mkConfig(['scaling' => new ConstantScaling(3)]);
        $stream = new Stream($cfg);

        foreach (range(1, 10) as $i) {
            $stream->push("m$i", [$i]);
        }

        $start = Time::nowMs();
        $rs = [];
        $f = function (array $_) use (&$rs, $start) {
            $rs[] = Time::nowMs() - $start;
            delay(20);
        };
        $h = new TestHandler($f);

        $stream->run($h, 10);

        $isNearEqualFn = static fn(array $p) => ($p[1] - $p[0]) >= 15;
        $arr = array_map($isNearEqualFn, Arr::slidingWindow($rs, 2));
        foreach (array_chunk($arr, 3) as $trio) {
            $this->assertEquals([false, false, true], $trio);
        }

        $stream->close();
    }

    public function testTimeout(): void
    {
        $cfg = $this->mkConfig(['timeoutJob' => new Milli(50)]);
        $stream = new Stream($cfg);

        $stream->push("m1", ['one']);

        $neverReach = true;
        $f = static function (array $msg) use (&$neverReach) {
            delay(100);
            $neverReach = false;
        };
        $h = new TestHandler($f);

        $stream->run($h, 1);

        $this->assertTrue($neverReach);

        $stream->close();
    }

    public function testAutoScaling(): void
    {
        $scale = new TestScaling();

        $cfg = $this->mkConfig(['scaling' => $scale]);
        $stream = new Stream($cfg);

        foreach (range(1, 10) as $i) {
            $stream->push("m$i", [$i]);
        }

        $start = Time::nowMs();
        $rs = [];
        $f = static function (array $_) use (&$rs, $start) {
            $rs[] = Time::nowMs() - $start;
            delay(20);
        };
        $h = new TestHandler($f);

        $stream->run($h, 10);

        $isNearEqualFn = static fn(array $p) => ($p[1] - $p[0]) >= 15 ? 1 : 0;
        $arr = array_map($isNearEqualFn, Arr::slidingWindow($rs, 2));

        $this->assertEquals([0, 0, 1, 0, 0, 1, 1, 1, 1], $arr);

        $stream->close();
    }

    public function testDeadLetterQueue(): void
    {
        $props = [
            'stream' => 's',
            'blockRead' => new Milli(100),
            'autoClaimMinIdle' => new Milli(1),
            'deduplicationTtl' => new Sec(5),
            'maxRetries' => 2,
            'retryInterval' => new Milli(10),
            'claimInterval' => new Milli(10),
        ];

        $cfg1 = $this->mkConfig(array_merge($props, ['group' => 'g1']));
        $stream = new Stream($cfg1);

        $predis = $this->mkRedis();
        $predis->execute(['XGROUP', 'CREATE', 's', 'g2', '$']);

        $stream->push('m1', ['one']);

        $f = static function (array $msg) {
            throw new Exception('COROUTINE FAILED');
        };

        $stream->run(new TestHandler($f), 3);

        $r1 = $this->xReadGroup('g2', 'c', 10, 100, 's', '>');
        $this->assertCount(3, $r1['s']);

        $stream->push('m2', ['two']); // only for call autoclame by interval
        $stream->run(new TestHandler($f), 3);
        $stream->close();

        $q2 = $this->xReadGroup('g1', 'c', 10, 100, 's:dlq', '>');
        $this->assertCount(1, $q2['s:dlq']);
    }

    protected function tearDown(): void
    {
        $this->mkRedis()->execute(['flushAll']);
    }

    protected function tearUp(): void
    {
        $this->mkRedis()->execute(['flushAll']);
    }

    private function mkConfig(array $options = []): Config
    {
        $default = [
            'redisUrl' => 'redis://redis:6379',
            'stream' => 's',
            'group' => 'g',
            'consumer' => 'c',
            'healthcheckInterval' => new Sec(30),
            'logger' => new NullLogger(),
//            'logger' => new ConsoleLogger(),
            'maxStreamLength' => 100,
            'maxDlqStreamLength' => 100,
            'readRetrySetCount' => 100,
            'readAutoClaimCount' => 100,
            'blockRead' => new Sec(1),
            'deduplicationTtl' => new Sec(1),
            'autoClaimMinIdle' => new Sec(60),
            'maxRetries' => 3,
            'retry' => new ConstantRetry(),
            'scaling' => new ConstantScaling(10),
            'retryInterval' => new Sec(1),
            'claimInterval' => new Sec(1),
            'timeoutJob' => new Sec(50),
            'minRedisWorkers' => 1,
            'maxRedisWorkers' => 100,
            'threadBootloader' => static function () {
                require  '/usr/src/app/examples/bootstrap.php';
                require  '/usr/src/app/vendor/autoload.php';
            },
        ];

        $args = array_merge($default, $options);

        return Config::fromArray($args);
    }

    private function mkRedis(): ClientInterface
    {
        return PredisClientFactory::fromConfig($this->mkConfig())->make();
    }

    private function xReadGroup(string $group, string $consumer, int $count, int $blockMs, string ...$keyAndId): array
    {
        $args = ['XREADGROUP', 'GROUP', $group, $consumer, 'COUNT', $count, 'BLOCK', $blockMs, 'STREAMS', ...$keyAndId];

        $raw = $this->mkRedis()->execute($args);
        if ($raw === null) {
            return [];
        }

        $result = [];
        foreach ($raw as $row) {
            foreach (Arr::listPairsToArray($row, stub: []) as $stream => $rowValues) {
                foreach ($rowValues as $rowValue) {
                    foreach (Arr::listPairsToArray($rowValue, stub: []) as $id => $fields) {
                        $result[$stream][$id] = Arr::listPairsToArray($fields);
                    }
                }
            }
        }

        return $result;
    }
}


