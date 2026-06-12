<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Retry;

use Glider88\AsyncRedisStreams\TimeInterval\TimeIntervalInterface;

interface RetryInterface
{
    public function delay(int $step): TimeIntervalInterface;
}
