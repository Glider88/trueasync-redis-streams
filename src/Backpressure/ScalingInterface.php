<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Backpressure;

interface ScalingInterface
{
    public function numberOfWorkers(int $lag): int;
}
