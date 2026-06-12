<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Backpressure;

use Glider88\AsyncRedisStreams\Helpers\Arr;
use InvalidArgumentException;

readonly class PiecewiseLinearScaling implements ScalingInterface
{
    /** @var non-empty-list<array{int, int, int, int, float}> */
    private array $saved;

    /** @param non-empty-array<int, int> $workerToLagPoints */
    public function __construct(array $workerToLagPoints)
    {
        if (count($workerToLagPoints) < 2) {
            throw new InvalidArgumentException('Incorrect number of configuration array');
        }

        $ks = array_keys($workerToLagPoints);
        if (min($ks) <= 0) {
            throw new InvalidArgumentException('At least one worker is needed');
        }

        if (max($ks) - min($ks) === 0) {
            throw new InvalidArgumentException('There is no delta for worker numbers');
        }

        $vs = array_values($workerToLagPoints);
        if (min($vs) < 0) {
            throw new InvalidArgumentException('Lag must be positive');
        }

        if (max($vs) - min($vs) === 0) {
            throw new InvalidArgumentException('There is no delta for lag');
        }

        $pairs = Arr::slidingWindow($workerToLagPoints, 2, true);

        $save = [];
        foreach ($pairs as $wl) {
            $w1 = array_key_first($wl);
            $w2 = array_key_last($wl);
            $dw = $w2 - $w1;
            if ($dw === 1) {
                $save[] = [$wl[$w1], $wl[$w2], $w1, $w2, 1];
                continue;
            }

            $l1 = $wl[$w1];
            $l2 = $wl[$w2];
            $dl = $l2 - $l1;
            $k = $dw / $dl;

            $save[] = [$l1, $l2, $w1, $w2, $k];
        }

        $this->saved = $save;
    }

    public function numberOfWorkers(int $lag): int
    {
        $first = array_first($this->saved);
        if ($lag <= $first[0]) {
            return $first[2];
        }

        $last = array_last($this->saved);
        if ($lag >= $last[1]) {
            return $last[3];
        }

        foreach ($this->saved as [$l1, $l2, $w1, $w2, $k]) {
            if ($lag >= $l1 && $lag <= $l2) {
                return (int) floor($w1 + ($lag - $l1) * $k);
            }
        }
    }
}
