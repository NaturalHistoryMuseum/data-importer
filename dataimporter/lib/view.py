import abc
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Iterable, Optional, List, Set, Any

from splitgill.utils import now, partition

from dataimporter.lib.dbs import ChangeQueue, EmbargoQueue, DataDB, Index
from dataimporter.lib.model import SourceRecord


@dataclass
class FilterResult:
    """
    Class representing the result of filtering a record in a view.

    We could use bools for this but having a class allows us to associate details too
    which is helpful for debugging why something is or isn't published on the Portal for
    example.
    """

    success: bool
    reason: Optional[str] = None

    def __bool__(self):
        return self.success


# reusable success FilterResult object
SUCCESS_RESULT = FilterResult(True)


class View:
    """
    A view on a data DB which provides a few different things, primarily:

    - filtering to determine what counts as a member of this view
    - transformation to convert a data DB SourceRecord object into a publishable
      record
    - tracking of records that change in the data DB that are members of this view
    - embargo tracking
    - links from this view to other and from others to this view as well as the
      propagation of changes through those links
    """

    def __init__(self, path: Path, db: DataDB):
        """
        :param path: the root path that all view related data should be stored under
        :param db: the DataDB object that backs this view
        """
        self.path = path
        self.db = db
        self.name = path.name
        self.changes = ChangeQueue(self.path / "changes")
        self.embargoes = EmbargoQueue(self.path / "embargoes")
        self.view_links_as_base: Set[ViewLink] = set()
        self.view_links_as_foreign: Set[ViewLink] = set()

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Given a record, determine whether it is a member of this view. Return a
        FilterResult with a success=True attribute if yes, otherwise return a
        FilterResult with a success=False attribute and a reason describing why the
        record isn't a member of this view.

        By default, this function returns a FilterResult with success=True.

        :param record: the SourceRecord to check
        :return: a FilterResult object
        """
        return SUCCESS_RESULT

    def make_data(self, record: SourceRecord) -> dict:
        """
        Given a record, return a dict containing the data to be ingested by Splitgill.
        This is likely to be the representation that makes it to the Data Portal, if
        indeed this view is published in that way. Linked data shouldn't be added in
        this method as this will be done automatically by the transform method.

        By default, this method returns the unmodified data from the source record.

        This method does not need to deal with deletions as it will not be passed them
        by the transform method.

        :param record: the SourceRecord object to transform
        :return: a dict
        """
        return record.data

    def transform(self, record: SourceRecord) -> dict:
        """
        Given a record, return a dict which will be ingested by Splitgill. This is
        therefore likely to be the representation that makes it to the Data Portal.

        The dict returned by this method contains both the core data and any linked data
        too. Nones are stripped by this method as well.

        :param record: a SourceRecord object
        :return: a dict
        """
        if record.is_deleted:
            return {}

        # make the data and strip any Nones
        data = {k: v for k, v in self.make_data(record).items() if v is not None}
        # augment with links if there are any
        for view_link in self.view_links_as_base:
            view_link.transform(record, data)
        return data

    def find_and_transform(self, ids: Iterable[str]) -> Iterable[dict]:
        """
        Convenience function which finds the records in this view which have the given
        IDs, passes them through the transform method, and then yields them.

        If any IDs aren't in this view, can't be found, are embargoed, or are deleted
        they will be ignored.

        :param ids: the ids to look up
        :return: an iterable of dicts
        """
        embargo_timestamp = now()
        return map(
            self.transform,
            (
                record
                for record in self.db.get_records(
                    record_id
                    for record_id in ids
                    if not self.embargoes.is_embargoed(record_id, embargo_timestamp)
                )
                if not record.is_deleted and self.is_member(record)
            ),
        )

    def get_and_transform(self, record_id: str) -> Optional[dict]:
        """
        Convenience function which finds the record in this view with the given ID,
        passes it to the transform method, and then returns the result. If the record
        can't be found, aren't members of this view, are embargoed, or deleted, then
        None is returned and transform isn't called.

        :param record_id: the record ID to look up and transform
        :return: the data dict from the transform call or None
        """
        if self.embargoes.is_embargoed(record_id, now()):
            return None
        record = self.db.get_record(record_id)
        if record is None or record.is_deleted or not self.is_member(record):
            return None
        return self.transform(record)

    def link(self, view_link: "ViewLink"):
        """
        Link this view ot the view link. In reality, this view is already in the view
        link object so all this actually does is store the view link object in the
        appropriate attribute set depending on whether this view is the base view in the
        view link or the foreign view.

        :param view_link: a ViewLink object
        """
        if view_link.base_view is self:
            self.view_links_as_base.add(view_link)
        else:
            self.view_links_as_foreign.add(view_link)

    def queue(self, records: List[SourceRecord]):
        """
        Given a set of records, update the changed records queue in this view. This will
        add any changed records to the queue, as well as any deletions. Changes will be
        propagated to the views linked to this view.

        This method doesn't account for records that have changed view. I.e. were a
        member of this view and then changed to become a member of a different view.
        This could be accounted for but given how often this is likely to happen, it's a
        big performance penalty to deal with it when it is really unlikely to happen.

        :param records: the records to check and queue
        """
        members = [record for record in records if self.is_member(record)]
        if members:
            self.changes.put_many(members)
            self.embargoes.put_many(members)
            for view_link in self.view_links_as_base:
                view_link.update_from_base(members)
            for view_link in self.view_links_as_foreign:
                view_link.update_from_foreign(members)

        deleted = [record for record in records if record.is_deleted]
        if deleted:
            self.changes.put_many(deleted)
            for view_link in self.view_links_as_foreign:
                view_link.update_from_foreign(deleted)

    def queue_new_releases(self):
        """
        Queues any records that have fallen out of their embargo.

        The record embargoes are compared to the current datetime.
        """
        # TODO: does this need to check redactions?
        # update the change queue with the released embargo ids (in chunks, which is
        # probably unnecessary but better safe than sorry)
        up_to = now()
        for chunk in partition(self.embargoes.iter_released(up_to), 5000):
            self.queue(list(self.db.get_records(chunk)))
        self.embargoes.flush_released(up_to)

    def count(self) -> int:
        """
        Count the number of record IDs in this view's change queue.

        :return: the number of IDs in the change queue
        """
        return self.changes.size()

    def iter_changed(self) -> Iterable[SourceRecord]:
        """
        Iterate over the IDs in the change queue and yield the corresponding
        SourceRecord objects.

        :return: yields SourceRecord objects
        """
        # iterate over the changed record IDs, yielding the full records
        yield from self.db.get_records(self.changes)

    def flush(self):
        """
        Remove all expired embargo IDs from the embargo queue and remove all IDs from
        the main view queue.
        """
        self.embargoes.flush_released(now())
        self.changes.clear()

    def rebuild(self):
        """
        Remove all data and then reread the entire source database to rebuild it.
        """
        # clear everything out
        self.changes.clear()
        self.embargoes.clear()
        for view_link in self.view_links_as_base:
            view_link.clear_from_base()
        for view_link in self.view_links_as_foreign:
            view_link.clear_from_foreign()

        # reload from source
        for chunk in partition(self.db, 5000):
            self.queue(chunk)

    def close(self):
        """
        Close any open connections.
        """
        self.embargoes.close()
        self.changes.close()
        for view_link in chain(self.view_links_as_base, self.view_links_as_foreign):
            view_link.close()


class ViewLink(abc.ABC):
    """
    Abstract class representing a link between two views. The objective of this class is
    twofold, firstly it provides a relationship model to track changes between the two
    views. When something changes on the linked view, the linked records on the base
    view need to be added to the base view's change queue. Additionally, this change
    needs to then propagate to any views linked to the base view and so on. This is
    handled through the "update_from_base" and "update_from_foreign" methods in this
    class. Secondly, it provides the transformation of the linked record's data into the
    base record's data. For example, linked records may be nested, or have their data
    merged. This is all handled by the "transform" function.

    The base view in this class contains the records which will have the linked view's
    record data added to them. For example, for our multimedia relationships the base
    view is the specimen and the foreign view is the images. The images are added to the
    base view's specimen records in the associatedMedia field. Typically, the base view
    records also contain the data that defines the links (e.g. the IDs of the linked
    records) but it doesn't have to.
    """

    def __init__(self, name: str, base_view: View, foreign_view: View):
        """
        :param name: a unique name for the view link
        :param base_view: the base view object
        :param foreign_view: the foreign view object
        """
        self.name = name
        self.base_view = base_view
        self.foreign_view = foreign_view
        # add this object to the base and foreign views
        self.base_view.link(self)
        self.foreign_view.link(self)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ViewLink):
            return self.name == other.name
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.name)

    @abc.abstractmethod
    def update_from_base(self, base_records: List[SourceRecord]):
        """
        Update the view link with data from the changed base records.

        :param base_records: a list of base SourceRecord object
        """
        ...

    @abc.abstractmethod
    def update_from_foreign(self, foreign_records: List[SourceRecord]):
        """
        Update the view link with data from the changed foreign records.

        :param foreign_records: a list of base SourceRecord object
        """
        ...

    @abc.abstractmethod
    def transform(self, base_record: SourceRecord, data: dict):
        """
        Given a base record and the transformed data created from it by the base view,
        combine the linked data from the foreign view with it.

        :param base_record: the base SourceRecord
        :param data: the transformed base record
        """
        ...

    def clear_from_base(self):
        """
        Clear the data in this view link that has been sourced from the base view.
        """
        pass

    def clear_from_foreign(self):
        """
        Clear the data in this view link that has been sourced from the foreign view.
        """
        pass

    def close(self):
        """
        Close any databases/files/etc associated with this view link.
        """
        pass


class ManyToOneViewLink(ViewLink, abc.ABC):
    """
    Represents a many-to-one link between two views.

    Base records have a link to at most one foreign record, but many foreign records can
    link back to the same base record.
    """

    def __init__(self, path: Path, base_view: View, foreign_view: View, field: str):
        """
        :param path: the root path where the ID map database will be stored
        :param base_view: the base view
        :param foreign_view: the foreign view
        :param field: field on base records which contains the linked foreign record ID
        """
        super().__init__(path.name, base_view, foreign_view)
        self.path = path
        self.field = field
        # a many-to-one index from base id -> foreign id
        self.id_map = Index(path / "id_map")

    def update_from_base(self, base_records: List[SourceRecord]):
        """
        Extracts the linked foreign ID from each of the given records and adds them to
        the ID map.

        :param base_records: the changed base records
        """
        self.id_map.put_many(
            (base_record.id, foreign_id)
            for base_record in base_records
            if (foreign_id := base_record.get_first_value(self.field))
        )

    def update_from_foreign(self, foreign_records: List[SourceRecord]):
        """
        Propagate the changes in the given foreign records to the base records linked to
        them.

        :param foreign_records: the updated foreign records
        """
        # do a reverse lookup to find the potentially many base IDs associated with each
        # updated foreign ID, and store them in a set
        base_ids = {
            base_id
            for foreign_record in foreign_records
            for base_id in self.id_map.get_keys(foreign_record.id)
        }

        if base_ids:
            base_records = list(self.base_view.db.get_records(base_ids))
            if base_records:
                # if there are associated base records, queue changes to them on the
                # base view
                self.base_view.queue(base_records)

    def get_foreign_record_data(self, base_record: SourceRecord) -> Optional[dict]:
        foreign_id = base_record.get_first_value(self.field)
        if foreign_id:
            return self.foreign_view.get_and_transform(foreign_id)

    def clear_from_base(self):
        """
        Clears out the ID map.
        """
        self.id_map.clear()


class ManyToManyViewLink(ViewLink, abc.ABC):
    """
    Represents a many-to-many link between two views.

    Base records have a link to many foreign records and many foreign records can link
    back to the same base record.
    """

    def __init__(self, path: Path, base_view: View, foreign_view: View, field: str):
        """
        :param path: the root path where the ID map database will be stored
        :param base_view: the base view
        :param foreign_view: the foreign view
        :param field: field on base records which contains the linked foreign record ID
        """
        super().__init__(path.name, base_view, foreign_view)
        self.path = path
        self.field = field
        # a many-to-many index from base id -> foreign id
        self.id_map = Index(path / "id_map")

    def update_from_base(self, base_records: List[SourceRecord]):
        """
        Update the ID map with the IDs of each base record provided along with the IDs
        of the foreign records that are linked to each base record. This is a many-to-
        many mapping as multiple foreign IDs can be present on each base record.

        :param base_records: the updated base records
        """
        self.id_map.put_multiple_values(
            (record.id, record.iter_all_values(self.field)) for record in base_records
        )

    def update_from_foreign(self, foreign_records: List[SourceRecord]):
        """
        Propagate the changes in the given foreign records to the base records linked to
        them.

        :param foreign_records: the updated foreign records
        """
        # do a reverse lookup to find the potentially many base IDs associated with each
        # updated foreign ID, and store them in a set
        base_ids = {
            base_id
            for foreign_record in foreign_records
            for base_id in self.id_map.get_keys(foreign_record.id)
        }

        if base_ids:
            base_records = list(self.base_view.db.get_records(base_ids))
            if base_records:
                # if there are associated base records, queue changes to them on the
                # base view
                self.base_view.queue(base_records)

    def get_foreign_record_data(self, base_record: SourceRecord) -> List[dict]:
        """
        Returns a list of transformed foreign records which are linked to the given base
        record. If no records are linked, an empty list is returned.

        :param base_record: the base record
        :return: a list of the transformed foreign data
        """
        return list(
            self.foreign_view.find_and_transform(
                base_record.iter_all_values(self.field)
            )
        )

    def clear_from_base(self):
        """
        Clears out the ID map.
        """
        self.id_map.clear()
