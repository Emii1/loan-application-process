class Interval:
    def __init__(self, start, end, data=None):
        if start >= end:
            raise ValueError("Start must be less than end")
        self.start = start
        self.end = end
        self.data = data or {}

    def overlaps(self, qstart, qend):
        return self.start < qend and self.end > qstart

    def __repr__(self):
        return f"Interval({self.start}, {self.end}, data={self.data})"


class IntervalTree:
    class Node:
        def __init__(self, interval):
            self.interval = interval
            self.max_end = interval.end
            self.left = None
            self.right = None

    def __init__(self):
        self.root = None

    def add(self, interval):
        self.root = self._insert(self.root, interval)

    def _insert(self, node, interval):
        if node is None:
            return self.Node(interval)

        if interval.start < node.interval.start:
            node.left = self._insert(node.left, interval)
        else:
            node.right = self._insert(node.right, interval)

        # Update max_end
        node.max_end = node.interval.end
        if node.left:
            node.max_end = max(node.max_end, node.left.max_end)
        if node.right:
            node.max_end = max(node.max_end, node.right.max_end)

        return node

    def __getitem__(self, slc):
        if not isinstance(slc, slice):
            raise ValueError("Only slicing is supported")
        if slc.step is not None:
            raise ValueError("Step in slicing is not supported")
        qstart, qend = slc.start, slc.stop
        if qstart >= qend:
            return []
        return self._query(self.root, qstart, qend)

    def _query(self, node, qstart, qend):
        if node is None:
            return []

        result = []
        if node.interval.overlaps(qstart, qend):
            result.append(node.interval)

        if node.left and node.left.max_end >= qstart:
            result.extend(self._query(node.left, qstart, qend))

        if node.right:
            result.extend(self._query(node.right, qstart, qend))

        return result
