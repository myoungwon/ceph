import { Helper } from '../helper.po';
import { UsersPageHelper } from './users.po';

describe('RGW users page', () => {
  let users: UsersPageHelper;
  const user_name = '000user_create_edit_delete';

  beforeAll(() => {
    users = new UsersPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await users.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await expect(users.getBreadcrumbText()).toEqual('Users');
    });
  });

  describe('create, edit & delete user test', () => {
    beforeAll(async () => {
      await users.navigateTo();
    });

    it('should create user', async () => {
      await users.create(user_name, 'Some Name', 'original@website.com', '1200');
      await expect(users.getTableCell(user_name).isPresent()).toBe(true);
    });

    it('should edit users full name, email and max buckets', async () => {
      await users.edit(user_name, 'Another Identity', 'changed@othersite.com', '1969');
      // checks for succsessful editing are done within edit function
    });

    it('should delete user', async () => {
      await users.delete(user_name);
      await expect(users.getTableCell(user_name).isPresent()).toBe(false);
    });
  });

  describe('Invalid input test', () => {
    beforeAll(async () => {
      await users.navigateTo();
    });

    it('should put invalid input into user creation form and check fields are marked invalid', async () => {
      await users.invalidCreate();
    });

    it('should put invalid input into user edit form and check fields are marked invalid', async () => {
      await users.invalidEdit();
    });
  });
});
