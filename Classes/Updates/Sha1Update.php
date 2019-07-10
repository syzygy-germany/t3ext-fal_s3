<?php
namespace MaxServ\FalS3\Updates;

/*
 * This file is part of the TYPO3 CMS project.
 *
 * It is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, either version 2
 * of the License, or any later version.
 *
 * For the full copyright and license information, please read the
 * LICENSE.txt file that was distributed with this source code.
 *
 * The TYPO3 project - inspiring people to share!
 */

use MaxServ\FalS3\Driver\AmazonS3Driver;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Registry;
use TYPO3\CMS\Core\Resource\Index\FileIndexRepository;
use TYPO3\CMS\Core\Resource\ResourceFactory;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Install\Updates\AbstractUpdate;

/**
 * Updates the sha1 hashes of sys_file and sys_file_processedfile records to avoid regeneration of the thumbnails
 */
class Sha1Update extends AbstractUpdate
{
    /**
     * @var string
     */
    protected $title = '[Optional] Update sys_file and sys_file_processedfile records to match new sha1 calculation.';

    /**
     * Checks if an update is needed
     *
     * @param string &$description The description for the update
     * @return bool Whether an update is needed (TRUE) or not (FALSE)
     */
    public function checkForUpdate(&$description)
    {
        $description = 'The hash calculation for files (content) has been changed.'
            . ' This means that your processed files need to be updated.<br />'
            . ' This can either happen on demand, when the processed file is first needed, or by executing this'
            . ' wizard, which updates all processed files at once.<br />'
            . '<strong>Important:</strong> If you have lots of processed files, you should prefer using this'
            . ' wizard, otherwise this might cause a lot of work for your server.';

        if ($this->isWizardDone()) {
            return false;
        }

        // Check if there is a registry entry from a former run that may have been stopped
        $registry = GeneralUtility::makeInstance(Registry::class);
        $registryEntry = $registry->get('falS3', 'sha1Update');
        if ($registryEntry !== null) {
            return true;
        }

        $queryBuilder = GeneralUtility::makeInstance(ConnectionPool::class)->getQueryBuilderForTable('sys_file');
        $expressionBuilder = $queryBuilder->expr();
        $incompleteCount = $queryBuilder->count('sys_file.uid')
            ->from('sys_file')
            ->innerJoin(
                'sys_file',
                'sys_file_storage',
                'sys_file_storage',
                $expressionBuilder->eq(
                    'sys_file.storage',
                    $queryBuilder->quoteIdentifier('sys_file_storage.uid')
                )
            )
            ->where(
                $expressionBuilder->eq(
                    'sys_file_storage.driver',
                    $queryBuilder->createNamedParameter(AmazonS3Driver::DRIVER_KEY, \PDO::PARAM_STR)
                ),
                $expressionBuilder->eq(
                    'sys_file.missing',
                    $queryBuilder->createNamedParameter(0, \PDO::PARAM_INT)
                )
            )->execute()->fetchColumn(0);

        return (bool)$incompleteCount;
    }

    /**
     * Performs the update
     *
     * @param array &$databaseQueries Queries done in this update
     * @param string &$customMessage Custom message
     * @return bool
     */
    public function performUpdate(array &$databaseQueries, &$customMessage)
    {
        $connectionPool = GeneralUtility::makeInstance(ConnectionPool::class);
        $queryBuilder = $connectionPool->getConnectionForTable('sys_file')->createQueryBuilder();
        $expressionBuilder = $queryBuilder->expr();

        $fileIndexRepository = FileIndexRepository::getInstance();
        $resourceFactory = GeneralUtility::makeInstance(ResourceFactory::class);

        $registry = GeneralUtility::makeInstance(Registry::class);
        $firstUid = $registry->get('falS3', 'sha1Update');

        $queryBuilder = $queryBuilder->select('sys_file.*')
            ->from('sys_file')
            ->innerJoin(
                'sys_file',
                'sys_file_storage',
                'sys_file_storage',
                $expressionBuilder->eq(
                    'sys_file.storage',
                    $queryBuilder->quoteIdentifier('sys_file_storage.uid')
                )
            )
            ->where(
                $expressionBuilder->eq(
                    'sys_file_storage.driver',
                    $queryBuilder->createNamedParameter(AmazonS3Driver::DRIVER_KEY, \PDO::PARAM_STR)
                ),
                $expressionBuilder->eq(
                    'sys_file.missing',
                    $queryBuilder->createNamedParameter(0, \PDO::PARAM_INT)
                )
            )
            ->orderBy('sys_file.uid');
        if ((int)$firstUid > 0) {
            $queryBuilder->andWhere(
                $queryBuilder->expr()->gt(
                    'sys_file.uid',
                    $queryBuilder->createNamedParameter($firstUid, \PDO::PARAM_INT)
                )
            );
        }
        $statement = $queryBuilder->execute();
        while ($row = $statement->fetch()) {
            try {
                $file = $resourceFactory->getFileObject($row['uid']);
            } catch (\Exception $e) {
                $fileIndexRepository->markFileAsMissing($row['uid']);
                continue;
            }

            if (!$file->exists()) {
                $fileIndexRepository->markFileAsMissing($row['uid']);
                continue;
            }

            $sha1 = $file->getStorage()->hashFile($file, 'sha1');

            if ($sha1 !== $row['sha1']) {
                $updateQueryBuilder = $connectionPool->getConnectionForTable('sys_file')->createQueryBuilder();
                $updateQueryBuilder->update('sys_file')
                    ->where(
                        $updateQueryBuilder->expr()->eq(
                            'uid',
                            $updateQueryBuilder->createNamedParameter($row['uid'], \PDO::PARAM_INT)
                        )
                    )
                    ->set('sha1', $sha1);
                $databaseQueries[] = $updateQueryBuilder->getSQL();
                $updateQueryBuilder->execute();
                $updateQueryBuilder = $connectionPool->getConnectionForTable('sys_file_processedfile')->createQueryBuilder();
                $updateQueryBuilder->update('sys_file_processedfile')
                    ->where(
                        $updateQueryBuilder->expr()->eq(
                            'original',
                            $updateQueryBuilder->createNamedParameter($row['uid'], \PDO::PARAM_INT)
                        )
                    )
                    ->set('originalfilesha1', $sha1);
                $databaseQueries[] = $updateQueryBuilder->getSQL();
                $updateQueryBuilder->execute();
            }
            $registry->set('falS3', 'sha1Update', (int)$row['uid']);
        }

        $registry->remove('falS3', 'sha1Update');
        $this->markWizardAsDone();

        return true;
    }
}
