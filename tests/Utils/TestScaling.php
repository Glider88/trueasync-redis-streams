<?php declare(strict_types=1);

namespace Tests\Glider88\AsyncRedisStreams\Utils;

use Glider88\AsyncRedisStreams\Backpressure\ScalingInterface;

readonly class TestScaling implements ScalingInterface
{
    public function numberOfWorkers(int $lag): int
    {
        if ($lag < 5) {
            return 1;
        }

        return 3;
    }
}
