"""Several Airflow sources for one date, read as one store (Data Platform + Risk Team by default).

Every loaded record gets `airflow_source` (display name) and `airflow_url`; raw tables
gain an `airflow_source` column. DAG IDs are unique across the two Airflows today; if a
DAG ID ever appears in both, the snapshot carries a warning instead of hiding it.
Files on disk stay per source; nothing here writes Parquet.
"""
import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from lineage_app.data.operations import TABLES as OP_TABLES
from lineage_app.data.parquet import SCHEMAS


class MultiLock:
    """Acquire every source store's lock in a fixed order (no deadlock with single-store users)."""
    def __init__(self, locks):
        self.locks = sorted(set(locks), key=id)

    def __enter__(self):
        for lock in self.locks:
            lock.acquire()
        return self

    def __exit__(self, *exc):
        for lock in reversed(self.locks):
            lock.release()


class CombinedStore:
    def __init__(self, sources, day):
        """sources: [(name, ParquetStore day view)] for the same stage and date."""
        self.sources = list(sources)
        self.day = day
        self.lock = MultiLock(store.lock for _, store in self.sources)

    def incomplete(self):
        return any(store.incomplete() for _, store in self.sources)

    def _metas(self):
        return [(name, store, store.metadata()) for name, store in self.sources if store.metadata()]

    def metadata(self):
        metas = self._metas()
        if not metas:
            return None
        first = metas[0][2]
        coverage, operational, warnings = {}, {}, []
        for name, _, meta in metas:
            for key, value in (meta.get('coverage') or {}).items():
                if isinstance(value, int) and not isinstance(value, bool):
                    coverage[key] = coverage.get(key, 0) + value
            for key, state in (meta.get('operational_coverage') or {}).items():
                merged = operational.setdefault(key, {'state': 'complete'})
                for field, value in state.items():
                    if field == 'state':
                        if value != 'complete':
                            merged['state'] = 'partial' if value == 'partial' or merged['state'] != 'unavailable' else value
                    elif isinstance(value, int) and not isinstance(value, bool):
                        merged[field] = merged.get(field, 0) + value
                    else:
                        merged.setdefault(field, value)
            warnings.extend(f'{name}: {w}' for w in meta.get('warnings') or [])
        return dict(first, coverage=coverage, operational_coverage=operational, warnings=warnings,
                    collected_at=max(meta['collected_at'] for _, _, meta in metas),
                    base_url=first.get('base_url', '') if len(metas) == 1 else ' + '.join(m.get('base_url', '') for _, _, m in metas),
                    source_name=' + '.join(name for name, _, _ in metas),
                    sources=[dict(name=name, base_url=meta.get('base_url'), source_id=meta.get('source_id'),
                                  collected_at=meta.get('collected_at')) for name, _, meta in metas])

    def load(self, include_diagnostics=True):
        meta = self.metadata()
        if not meta:
            return None
        result = dict(metadata=meta, rows=[], inventory=[], definitions=[], dependencies=[], dag_links=[],
                      operations={key: [] for key in OP_TABLES})
        for name, store, source_meta in self._metas():
            data = store.load(include_diagnostics=include_diagnostics)
            tag = dict(airflow_source=name, airflow_url=source_meta.get('base_url', ''))
            for key in ('rows', 'inventory', 'definitions', 'dependencies', 'dag_links'):
                result[key].extend({**record, **tag} for record in data.get(key, []))
            for key, records in data['operations'].items():
                result['operations'].setdefault(key, []).extend({**r, **tag} if isinstance(r, dict) else r for r in records)
        seen = {}
        for dag in result['inventory']:
            seen.setdefault(dag['dag_id'], []).append(dag['airflow_source'])
        meta['warnings'].extend(f'DAG ID {dag} exists in {" and ".join(names)}; its records are shown together.'
                                for dag, names in sorted(seen.items()) if len(names) > 1)
        return result

    def dates(self):
        return sorted({d for _, store in self.sources for d in store.dates()}, reverse=True)

    def raw_catalog(self, day):
        catalogs = [store.raw_catalog(day) for _, store in self.sources]
        result = []
        for index, name in enumerate(SCHEMAS):
            entries = [catalog[index] for catalog in catalogs]
            present = [entry for entry in entries if entry['present']]
            fields = (present[0] if present else entries[0])['fields']
            result.append(dict(name=name, path=f'{name}/{day.isoformat()}.parquet · {len(present)} Airflow file(s)',
                               present=bool(present), rows=sum(e['rows'] for e in present) if present else None,
                               bytes=sum(e['bytes'] for e in present) if present else None,
                               fields=[*fields, dict(name='airflow_source', type='string', nullable=True)]))
        return result

    def raw_rows(self, dataset, day):
        return [{**row, 'airflow_source': name} for name, store in self.sources for row in store.raw_rows(dataset, day)]

    def raw_file(self, dataset, day):
        tables = []
        for name, store in self.sources:
            path = store.path(dataset, day)
            if path.is_file():
                table = pq.ParquetFile(path).read()
                tables.append(table.append_column('airflow_source', pa.array([name] * table.num_rows, pa.string())))
        if not tables:
            raise ValueError('No saved Parquet file for this table in the snapshot.')
        buffer = io.BytesIO()
        pq.write_table(pa.concat_tables(tables, promote_options='default'), buffer, compression='snappy')
        return buffer.getvalue()

    def task_diagnostics(self, dag, run, task):
        return [row for _, store in self.sources for row in store.task_diagnostics(dag, run, task)]
