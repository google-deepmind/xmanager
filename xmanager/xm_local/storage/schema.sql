-- Copyright 2021 DeepMind Technologies Limited
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Version history of the database.
-- Used to decide whether the database needs to upgrade.
CREATE TABLE VersionHistory (
  Version INTEGER,
  Timestamp TIMESTAMP,
  PRIMARY KEY(Version)
);

-- When this file is run for the first time, insert version 1 into the history.
INSERT INTO VersionHistory(Version, Timestamp) VALUES(1, strftime('%s', 'now'));

-- Experiment.
CREATE TABLE Experiment (
  Id INTEGER,
  PRIMARY KEY(Id)
);

-- Work Unit.
CREATE TABLE WorkUnit (
  ExperimentId INTEGER,
  WorkUnitId INTEGER,
  PRIMARY KEY(ExperimentId, WorkUnitId)
);

-- Job.
CREATE TABLE Job (
  ExperimentId INTEGER,
  WorkUnitId INTEGER,
  -- Name of the Job to identify it within the work unit.
  Name TEXT,
  -- Metadata of the Job.
  Data TEXT,
  PRIMARY KEY(ExperimentId, WorkUnitId, Name)
);
